"""
opensignal_job.py
~~~~~~~~~~

This Python module contains an opensignal job definition.

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    jobs/opensignal_job.py
"""
import json
import os
from sys import path

import __main__
from os import environ, listdir

from pyspark.files import SparkFiles
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, concat_ws, lit, row_number, date_format, avg, datediff
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.window import Window


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and configs
    spark, log = start_spark(
        app_name='opensignal_job',
        spark_config={"spark.driver.memory": "8g",
                      'spark.debug.maxToStringFields': '1000'})

    config_dict = load_config(log)
    csv_writer = CsvWriter(config_dict['output_base'])

    # log that main ETL job is starting
    log.warn('opensignal_job is up-and-running')

    # execute ETL pipeline
    taxi_df = load_data_parquet(spark, config_dict['ny_taxi_path'])

    # filter the ny_taxi data between the dates 2015-01-15 to 2015-02-15 using the tpep_pickup_datetime column.
    result = taxi_df.filter(F.col("tpep_pickup_datetime").between('2015-01-15', '2015-02-15'))
    # I additionally filtered data where trip_distance is <0
    result = result.where(col('trip_distance') > 0)

    # 5. Filter right to be forgotten taxi_ids. Remove all rows that have a taxi_id that is in the ny_taxi_rtbf list.
    taxi_rtbf_df = load_taxi_rtvf_df(spark, config_dict['ny_taxi_rbtf_path'])
    result = result.join(F.broadcast(taxi_rtbf_df), on=['taxi_id'], how='left_anti')

    # Insight 1: Calculate the average total fare for each trip_distance
    # (trip distance bin size of 1, or rounded to 0 decimal places) and the number of trips.
    # Order the output by trip_distance.Write the output as a single csv with headers.
    # The resulting output should have 3 columns: trip_distance, average_total_fare and number_of_trips.

    insight_1 = (result.groupby(F.round(result['trip_distance'], 0).cast('integer').alias('trip_distance'))
                 .agg(F.avg(result['fare_amount']).alias('average_total_fare'),
                      F.count(F.lit(1)).alias('number_of_trips')
                      )
                 )

    csv_writer.save_data_csv(insight_1, 'insight_1')

    # Looking at the output of step 9, decide what would be an appropriate upper limit of trip_distance and rerun with this filtered.
    # insight_1_ratio = insight_1.withColumn('price/mile', F.col('average_total_fare') / F.col('trip_distance')).where(
    #     col('number_of_trips') >= 3).orderBy('trip_distance')
    # csv_writer.save_data_csv(insight_1_ratio, 'insight_1_ratio')
    # I have picked 65 as the upper limit, because after that distance I spotted multiple values od price/mile <2
    result = result.where(result['trip_distance'] <= 65)

    result = enrich_dataset_with_zones_and_borough(config_dict, result, spark)

    # 11. Filter rows if any of the columns: pickup_zone, pickup_borough, dropoff_zone and dropoff_borough are null.
    result = (result.where((col('pickup_zone').isNotNull()
                            & col('pickup_borough').isNotNull()
                            & col('dropoff_zone').isNotNull()
                            & col('dropoff_borough').isNotNull()))
              )

    # select important columns and cache
    result = (result.select('pickup_zone', 'pickup_borough', 'dropoff_zone', 'fare_amount', 'trip_distance',
                            'dropoff_borough', 'tpep_pickup_datetime').cache())

    # Insight 2: Total number of pickups in each zone.
    insight_2 = (result.groupBy(F.col('pickup_zone').alias('zone'))
                 .agg(F.count(F.lit(1)).alias('number_of_pickups'))
                 )
    csv_writer.save_data_csv(insight_2, 'insight_2')

    # Insight 3: Total number of pickups in each borough.
    insight_3 = (result.groupBy(F.col('pickup_borough').alias('zone'))
                 .agg(F.count(F.lit(1)).alias('number_of_pickups'))
                 )
    csv_writer.save_data_csv(insight_3, 'insight_3')

    insight_4 = generate_insight_4(result)
    csv_writer.save_data_csv(insight_4, 'insight_4')

    insight_5 = generate_insight_5_total_dropoffs_and_averages_in_borough(result)
    csv_writer.save_data_csv(insight_5, 'insight_5')

    insight_6 = generate_insight_6_top_5_dropoffs_within_pickup_zone(result)
    csv_writer.save_data_csv(insight_6, 'insight_6')

    insight_7 = generate_insight_7_average_trips_per_hour(result)
    csv_writer.save_data_csv(insight_7, 'insight_7')

    # log the success and terminate Spark application
    log.warn('test_opensignal_job is finished')
    spark.stop()
    return None


def generate_insight_4(result):
    """
    Insight 4: Total number of dropoffs, average total cost and average distance in each zone. Write the output as
    a single csv with headers. The resulting output should have 4 columns:
    zone, number_of_dropoffs, average_total_fare and average_trip_distance.
    """
    insight_4 = (result.groupBy(F.col('dropoff_zone').alias('zone'))
                 .agg(F.avg(F.col('fare_amount')).alias('average_total_fare'),
                      F.avg(F.col('trip_distance')).alias('average_trip_distance'))
                 )
    return insight_4


def generate_insight_5_total_dropoffs_and_averages_in_borough(result):
    """
    Insight 5: Total number of dropoffs, average total cost and average distance in each borough.
    The resulting output should have 4 columns: borough, number_of_dropoffs, average_total_fare
    and average_trip_distance.
     """
    insight_5 = (result.groupBy(F.col('dropoff_borough').alias('borough'))
                 .agg(F.count(F.lit(1)).alias('number_of_dropoffs'),
                      F.avg(F.col('fare_amount')).alias('average_total_fare'),
                      F.avg(F.col('trip_distance')).alias('average_trip_distance'))
                 )
    return insight_5


def enrich_dataset_with_zones_and_borough(config_dict, input_df, spark):
    # 6. Using the geocoding data (ny_taxi_zones) and the appropriate index column in the ny_taxi data,
    # geocode each pickup location with zone and borough. We would like 2 new columns: pickup_zone and pickup_borough.
    zones_df = load_zones_df(spark, config_dict['ny_taxi_zones_path']).cache()
    result = (input_df.join(zones_df, input_df.pickup_h3_index == zones_df.h3_index_hex, how='left')
              .withColumnRenamed('zone', 'pickup_zone')
              .withColumnRenamed('borough', 'pickup_borough')
              .drop('h3_index_hex')
              )
    # 7.Using the geocoding data (ny_taxi_zones) and the appropriate index column in the ny_taxi data,
    # geocode each dropoff location with zone and borough. We would like 2 new columns: dropoff_zone and dropoff_borough.
    result = (result.join(zones_df, result.dropoff_h3_index == zones_df.h3_index_hex, how='left')
              .withColumnRenamed('zone', 'dropoff_zone')
              .withColumnRenamed('borough', 'dropoff_borough')
              .drop('h3_index_hex')
              )
    zones_df.unpersist()
    return result


def generate_insight_6_top_5_dropoffs_within_pickup_zone(result):
    """Insight 6: For each pickup zone calculate the top 5 dropoff zones ranked by number of trips.
    The resulting output should have 4 columns: pickup_zone, dropoff_zone, number_of_dropoffs and rank.
    """
    insight_6 = (result.groupBy('pickup_zone', 'dropoff_zone')
                 .agg(F.count(F.lit(1)).alias('number_of_dropoffs'))
                 )
    window_over_trip_distance = Window.partitionBy("pickup_zone").orderBy(col("number_of_dropoffs").desc(),
                                                                          col("dropoff_zone").asc())
    insight_6 = insight_6.withColumn("rank", row_number().over(window_over_trip_distance)).where(col('rank') <= 5)
    return insight_6


def generate_insight_7_average_trips_per_hour(result):
    """ Insight 7: Calculate the number of trips for each date -> pickup hour, (using tpep_pickup_datetime),
    then calculate the average number of trips by hour of day. The resulting output should have 2 columns:
     hour_of_day and average_trips.
    """
    days_count = (
        result.select(F.max('tpep_pickup_datetime').alias('max_date'), F.min('tpep_pickup_datetime').alias('min_date'))
            .withColumn('days_count', datediff('max_date', 'min_date') + 1)
            .first().asDict()['days_count']
    )
    insight_7 = (result.groupBy(date_format('tpep_pickup_datetime', 'HH').alias('hour_of_day'))
                 .agg((F.count(lit(1)) / lit(days_count)).alias('average_trips'))
                 )
    return insight_7


def load_taxi_rtvf_df(spark, csv):
    taxi_rtbf_df_schema = StructType([StructField("taxi_id", IntegerType(), True)])
    taxi_rtbf_df = load_data_csv(spark, csv,
                                 taxi_rtbf_df_schema)
    return taxi_rtbf_df


def load_zones_df(spark, zones_csv):
    zones_df_schema = (StructType([
        StructField("h3_index", LongType(), True),
        StructField("zone", StringType(), True),
        StructField("borough", StringType(), True)
    ]))
    zones_df = (load_data_csv(spark, zones_csv,
                              zones_df_schema)
                .withColumn('h3_index_hex', F.lower(F.hex(col('h3_index'))))
                .drop('h3_index')
                )
    return zones_df


def load_data_parquet(spark, path):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param path: path to files.
    :return: Spark DataFrame.
    """
    df = (
        spark
            .read
            .parquet(path))

    return df


def load_data_csv(spark, path, schema):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :param path: path to csv file.
    :return: Spark DataFrame.
    """
    df = (
        spark.read
            .options(header='True', delimiter=',', inferSchema=False)
            .schema(schema)
            .csv(path))

    return df


class CsvWriter(object):
    def __init__(self, base_path='./'):
        self.base_path = base_path

    def save_data_csv(self, df, relative_path):
        """Collect data locally and write to CSV.

        :param df: DataFrame to print.
        :param relative_path: str relative output path.
        :return: None
        """
        absolute_path = os.path.join(self.base_path, relative_path)
        (df
         .coalesce(1)
         .write
         .csv(absolute_path, mode='overwrite', header=True))
        return None


def load_config(spark_logger):
    # get configs file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = os.path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded configs from ' + config_files[0])
    else:
        spark_logger.warn('no configs file found using default')
        config_dict = ({
            "ny_taxi_path": "../tests/test_data/opensignal/ny_taxi_test_data/ny_taxi/*.parquet",
            "ny_taxi_rbtf_path": "../tests/test_data/opensignal/ny_taxi_test_data/ny_taxi_rtbf/rtbf_taxies.csv",
            "ny_taxi_zones_path": "../tests/test_data/opensignal/ny_taxi_test_data/ny_zones/ny_taxi_zones.csv",
            "output_base": "../results"
        })
    return config_dict


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load configs files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'configs.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for configs.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of configs key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        configs dict (only if available).
    """

    # detect execution environment
    flag_repl = not (hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder
                .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
                .builder
                .master(master)
                .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other configs params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    return spark_sess, spark_logger


class Log4j(object):
    """Wrapper class for Log4j JVM object.

    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.

        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.

        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.

        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
