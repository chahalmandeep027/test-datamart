import os.path

# write a function that takes all incessary info, read data from mysql and return a dataframe


def read_from_mysql(spark, app_secret, app_conf):
    print('\nReading data from MySQL DB,')

    jdbc_params = {"url": get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": app_conf["mysql_conf"]["dbtable"],
                   "numPartitions": "2",
                   "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }

    df = spark\
        .read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .options(**jdbc_params)\
        .load()

    return df

# write a function that takes all incessary info, read data from sftp and return a dataframe


def read_from_sftp(spark, app_secret, app_conf, pem_file_path):
    df = spark.read\
        .format("com.springml.spark.sftp")\
        .option("host", app_secret["sftp_conf"]["hostname"])\
        .option("port", app_secret["sftp_conf"]["port"])\
        .option("username", app_secret["sftp_conf"]["username"])\
        .option("pem", pem_file_path)\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(app_conf["sftp_conf"]["directory"] + "/" + app_conf["filename"])

    return df

# write a function that takes all incessary info, read data from mongodb and return a dataframe


def read_from_mongodb(spark, app_secret, app_conf):
    df = spark.read\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", app_secret["mongodb_config"]["uri"])\
        .option("database", app_conf["mongodb_config"]["database"])\
        .option("collection", app_conf["mongodb_config"]["collection"])\
        .load()

    return df

# write a function that takes all incessary info, read data from s3 and return a dataframe


def read_from_s3(spark, app_conf):
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .format("csv") \
        .load("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["filename"])

    return df


def read_from_s3_staging(spark, file_path):
    src_df = spark.sql(
        "select * from parquet.`{}`".format(file_path))
    return src_df

# write dataframe to Redshift


def read_from_redshift(spark, app_secret, app_conf, query):
    jdbc_url = get_redshift_jdbc_url(app_secret)
    print(jdbc_url)
    df = spark.read\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", jdbc_url) \
        .option("query", query) \
        .option("forward_spark_s3_credentials", "true")\
        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "temp")\
        .load()
    return df


def write_to_redshift(df, app_secret, s3_dir, table_name):
    df.coalesce(1).write\
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option('url', get_redshift_jdbc_url(app_secret))\
        .option("tempdir", s3_dir) \
        .option("forward_spark_s3_credentials", "true") \
        .option("dbtable", table_name) \
        .mode("overwrite")\
        .save()
    return


def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)
