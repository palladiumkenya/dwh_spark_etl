from pyspark.sql import SparkSession, functions as Fun
from configparser import ConfigParser
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
import pyodbc
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.getcwd(), ".env")
load_dotenv(dotenv_path=dotenv_path)


# setting up mssql db credentials
ip = os.getenv("ip")
database_name = os.getenv("database_name")
destination_db = os.getenv("destination_db")
dbUsr = os.getenv("dbUsr")
dbPwd = os.getenv("dbPwd")
dbDrvr = os.getenv("dbDrvr")
destination_dbtable = os.getenv("destination_dbtable")
# his list source
source_db = os.getenv("source_db")
source_db_user = os.getenv("source_db_user")
source_db_password = os.getenv("source_db_password")



def dbConnection():
    # sql server login url
    url = "jdbc:sqlserver://localhost;user=" + dbUsr + ";password=" + dbPwd + ";encrypt=true;trustServerCertificate=true;"

    # Establish a spark connection
    spark = SparkSession \
        .builder.master("local[*]").appName("pythonProject")\
        .config("spark.driver.extraClassPath", os.path.join(os.getcwd(), "drivers", "mysql-connector-java-8.0.11.jar;")+os.path.join(os.getcwd(), "drivers", "mssql-jdbc-10.2.1.jre8.jar")) \
        .getOrCreate()

    # establish pyodbc connection
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=' + ip + ';'
                        'Database=' + destination_db + ';'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()

    # =========================================================
    # ---------------------- load new logs --------------------

    source_logs_df = spark.read.format('jdbc') \
        .option("url", url + ";" + "databaseName=" + destination_db + ";") \
        .option("driver", dbDrvr) \
        .option("query", 'select * from '+destination_dbtable+'_Logs') \
        .option("user", dbUsr) \
        .option("password", dbPwd) \
        .option("sfSSL", "false") \
        .load()

    # get the last refresh date from the logs
    getRecentRefreshDate = source_logs_df.select("DateLastFreshed").orderBy(col("DateLastFreshed").desc()).limit(1)
    print('=============== LAST REFRESH DATE {0}==============='.format(str(getRecentRefreshDate.collect()[0].DateLastFreshed)))

    # ======================== load the query extracts =====================
    queryfile = os.path.join(os.getcwd(), "queries", "Load_All_EMR_Sites.txt")
    query = ""
    with open(queryfile, 'r') as file:
        query = file.read()

    pushdown_query = query

    # ======================== load the query extracts =====================
    # if logs are empty, a refresh has never been done so overwrite everything
    if len(source_logs_df.head(1)) == 0:
        source_extract_df = spark.read.format('jdbc') \
            .option("url",
                    "jdbc:mysql://localhost:3306/" + source_db + "?user=" + source_db_user + "&password=" + source_db_password + "") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("query",pushdown_query) \
            .option("user", "root") \
            .option("password", "DodgerMXT123") \
            .option("sfSSL", "false") \
            .load()

        source_extract_df.write.format("jdbc") \
            .option("url", url + ";" + "databaseName=" + destination_db + ";") \
            .option("user", dbUsr) \
            .option("dbtable", destination_dbtable)  \
            .option("password", dbPwd) \
            .option("driver", dbDrvr) \
            .option("sfSSL", "false") \
            .mode("overwrite").save()
        print('-----> overwrite')
    else:
        print('-----> append')
        # print((pushdown_query + " and facilities_facility_info.date_added > '{0}'").format(str(getRecentRefreshDate.collect()[0].DateLastFreshed)))

        # use refresh date to filter for most recent records  updates
        source_updates_df = spark.read.format('jdbc') \
            .option("url",
                    "jdbc:mysql://localhost:3306/" + source_db + "?user=" + source_db_user + "&password=" + source_db_password + "") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("query",
                    (pushdown_query + " and facilities_facility_info.date_modified > '{0}'").format(str(getRecentRefreshDate.collect()[0].DateLastFreshed))) \
            .option("user", "root") \
            .option("password", "DodgerMXT123") \
            .option("sfSSL", "false") \
            .load()

        # use refresh date to filter for most recent records additions
        #NOTE only select those where datecreated is null and datecreated is after DateLastFreshed!!!
        source_additions_df = spark.read.format('jdbc') \
            .option("url",
                    "jdbc:mysql://localhost:3306/" + source_db + "?user=" + source_db_user + "&password=" + source_db_password + "") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("query",
                    (pushdown_query + " and facilities_facility_info.date_added > '{0}'").format(str(getRecentRefreshDate.collect()[0].DateLastFreshed))) \
            .option("user", "root") \
            .option("password", "DodgerMXT123") \
            .option("sfSSL", "false") \
            .load()

        source_updates_df.show()
        source_additions_df.show()

        # remove records with updates from destination using mfl codes
        for update in source_updates_df.collect():
            dropquery = "delete from {0} where MFL_Code=?".format(destination_dbtable)
            cursor.execute(dropquery, (update["MFL_Code"],))
            conn.commit()

        joineddf = source_additions_df.union(source_updates_df)
        joineddf.show()
        # insert the updates and additions
        joineddf.write.format("jdbc") \
            .option("url", url + ";" + "databaseName=" + destination_db + ";") \
            .option("user", dbUsr) \
            .option("dbtable", destination_dbtable) \
            .option("password", dbPwd) \
            .option("driver", dbDrvr) \
            .option("sfSSL", "false") \
            .mode("append").save()


    # ---------- add refresh log after successfull addition ---------------
    newlog = "insert into {0} (MFL_Code,DateModified,DateLastFreshed,IsCurrent) values(99, null, '{1}',0)".format(destination_dbtable+'_Logs', datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    print('============== REFRESH LOGGED ===============')
    cursor.execute(newlog)
    conn.commit()
    conn.close()




if __name__ == "__main__":
    dbConnection()
