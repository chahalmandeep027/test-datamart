from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.pg.utils.utility as ut
import uuid
from pyspark.sql.types import StringType

if __name__ == "__main__":

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(
        current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(
        current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key",
                    app_secret["s3_conf"]["secret_access_key"])

    def fn_uuid():
        uid = uuid.uuid1()
        return str(uid)

    FN_UUID_UDF = spark.udf \
        .register("FN_UUID", fn_uuid, StringType())

# Read CP and Addr data for current date
    tgt_list = app_conf["target_list"]

    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]
        print(tgt)
        src_list = tgt_conf['sourceData']
        temp_dir = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "temp"
        for src in src_list:
            file_path = "s3a://" + \
                app_conf["s3_conf"]["s3_bucket"] + \
                app_conf["s3_conf"]["staging_dir"]+"/"+src
            src_df = ut.read_from_s3_staging(spark, file_path)
            src_df.printSchema()
            src_df.show(5, False)
            src_df.createOrReplaceTempView(src)

        src_tb_list = tgt_conf['sourceTable']
        for src_tb in src_tb_list:
            src_df = ut.read_from_redshift(
                spark, app_secret, temp_dir, "select * from " + src_tb)
            src_df.printSchema()
            src_df.show(5, False)
            src_df.createOrReplaceTempView(src)

        dim_df = spark.sql(app_conf[tgt]["loadingQuery"])
        dim_df.show(5)
        #
        # print(temp_dir)
        # ut.write_to_redshift(dim_df, app_secret, temp_dir,
        #  tgt_conf['tableName'])

        ut.write_to_redshift(dim_df, app_secret, temp_dir,
                             tgt_conf['tableName'])

        print(f'Writing {tgt} to redshift completed!')

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/pg/target_data_loading.py
# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
