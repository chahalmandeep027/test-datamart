# read data from mysql -  transactinsync table, create a dataframe out of it
# add a column 'ins_dt' - current_date()
# Write the dataframe in s3 partitioned by ind_dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.pg.utils.utility as ut

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

    src_list = app_conf["source_list"]

    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            print("Reading SB data from mysql")
            txnDF = ut.read_from_mysql(spark, app_secret, src_conf) \
                .withColumn('ins_dt', current_date())

            txnDF.show(5, False)

            txnDF.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/"+src)

            print("Writing SB data to s3 staging dir complete")

        elif src == "OL":
            print("Reading OL data from sftp")
            ol_txn_df = ut.read_from_sftp(spark, app_secret, src_conf,
                                          os.path.abspath(
                                              current_dir + "/../../" + app_secret["sftp_conf"]["pem"])) \
                .withColumn("ins_dt", current_date())

            ol_txn_df.show(5, False)

            ol_txn_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/" + src)

            print("Writing OL data to s3 staging dir complete")

        elif src == "CP":
            print("Reading CP data from s3")
            cp_df = ut.read_from_s3(
                spark, src_conf) \
                .withColumn("ins_dt", current_date())

            cp_df.show(5, False)

            cp_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt").parquet(
                    "s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/" + src)

            print("Writing CP data to s3 staging dir complete")

        elif src == "ADDR":
            print("Reading ADDR data from mongoDB")
            cust_addr_df = ut.read_from_mongodb(
                spark, app_secret, src_conf) \
                .withColumn("ins_dt", current_date())

            # cust_addr_df.printSchema()
            # cust_addr_df.show(5, False)

            cust_addr_df = cust_addr_df \
                .select(col("consumer_id"),
                        col("mobile-no"),
                        col("address.state").alias("state"),
                        col("address.city").alias("city"),
                        col("address.street").alias("street"),
                        "ins_dt")
            # print("dataframe after data curation")
            cust_addr_df.printSchema()
            cust_addr_df.show(5, False)

            cust_addr_df.write \
                .mode("overwrite") \
                .partitionBy("ins_dt") \
                .parquet(
                    "s3a://" + app_conf["s3_conf"]["s3_bucket"]+app_conf["s3_conf"]["staging_dir"]+"/" + src)

            print("Writing ADDR data to s3 staging dir complete")

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1" com/pg/source_data_loading.py
