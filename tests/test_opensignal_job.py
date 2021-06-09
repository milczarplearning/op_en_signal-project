"""
test_opensignal_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for certain transformations.
"""
import unittest

from jobs.opensignal_job import start_spark, generate_insight_7_average_trips_per_hour, \
    generate_insight_6_top_5_dropoffs_within_pickup_zone, generate_insight_5_total_dropoffs_and_averages_in_borough
from pyspark.sql import functions as F


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, *_ = start_spark()

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_insight_5_total_dropoffs_and_averages_in_borough(self):
        # given
        inputData = (
            ("dropoff_boroughZ", 1, 10, '2015-01-19 0:54:16.0'),
            ("dropoff_boroughZ", 2, 9, '2015-01-20 0:54:16.0'),
            ("dropoff_boroughZ", 3, 8, '2015-01-22 0:54:16.0'),
            ("dropoff_boroughZ", 4, 7, '2015-01-19 1:54:16.0'),
            ("dropoff_boroughY", 5, 6, '2015-01-19 1:54:16.0'),
            ("dropoff_boroughY", 6, 5, '2015-01-19 1:54:16.0'),
            ("dropoff_boroughY", 7, 4, '2015-01-20 12:54:16.0'),
            ("dropoff_boroughY", 8, 3, '2015-01-21 12:54:16.0'),
            ("dropoff_boroughX", 9, 2, '2015-01-19 12:54:16.0'),
            ("dropoff_boroughX", 10, 1, '2015-01-19 12:54:16.0')
        )

        input_columns = ['dropoff_borough', 'fare_amount',
                         'trip_distance',
                         'tpep_pickup_datetime']
        input = self.spark.createDataFrame(data=inputData, schema=input_columns)

        expected_data = (("dropoff_boroughZ", 4, 2.5, 8.5),
                         ("dropoff_boroughY", 4, 6.5, 4.5),
                         ("dropoff_boroughX", 2, 9.5, 1.1)
                         )
        expected_columns = ['borough', 'number_of_dropoffs', 'average_total_fare',
                         'average_trip_distance']

        expected_output = self.spark.createDataFrame(data=expected_data, schema=expected_columns)
        # when

        actual_output = generate_insight_5_total_dropoffs_and_averages_in_borough(input)

        # then
        self.assertTrue(are_dataframes_equal(actual_output, expected_output))

    def test_insight_6_top_5_dropoffs_within_pickup_zone(self):
        """Test data pipeline.
        """
        # given
        inputData = ((
                         ("pickup_zoneA", "dropoff_zoneA", "dropoff_boroughZ", 1, 10),
                         ("pickup_zoneA", "dropoff_zoneA", "dropoff_boroughZ", 2, 15),
                         ("pickup_zoneA", "dropoff_zoneA", "dropoff_boroughZ", 3, 20),
                         ("pickup_zoneA", "dropoff_zoneB", "dropoff_boroughZ", 4, 25),
                         ("pickup_zoneA", "dropoff_zoneB", "dropoff_boroughY", 5, 30),
                         ("pickup_zoneA", "dropoff_zoneC", "dropoff_boroughY", 6, 35),
                         ("pickup_zoneA", "dropoff_zoneC", "dropoff_boroughY", 7, 40),
                         ("pickup_zoneA", "dropoff_zoneD", "dropoff_boroughY", 8, 45),
                         ("pickup_zoneA", "dropoff_zoneE", "dropoff_boroughX", 9, 50),
                         ("pickup_zoneA", "dropoff_zoneF", "dropoff_boroughX", 10, 55)
                     )
                     +
                     (
                         ("pickup_zoneB", "dropoff_zoneA", "dropoff_boroughZ", 1, 10),
                         ("pickup_zoneB", "dropoff_zoneA", "dropoff_boroughZ", 2, 15),
                         ("pickup_zoneB", "dropoff_zoneA", "dropoff_boroughZ", 3, 20),
                         ("pickup_zoneB", "dropoff_zoneB", "dropoff_boroughZ", 4, 25),
                         ("pickup_zoneB", "dropoff_zoneB", "dropoff_boroughY", 5, 30),
                         ("pickup_zoneB", "dropoff_zoneC", "dropoff_boroughY", 6, 35),
                         ("pickup_zoneB", "dropoff_zoneC", "dropoff_boroughY", 7, 40),
                         ("pickup_zoneB", "dropoff_zoneD", "dropoff_boroughY", 8, 45),
                         ("pickup_zoneC", "dropoff_zoneE", "dropoff_boroughX", 9, 50),
                         ("pickup_zoneC", "dropoff_zoneF", "dropoff_boroughX", 10, 55)
                     ))
        input_columns = ['pickup_zone', 'dropoff_zone', 'dropoff_borough', 'fare_amount',
                         'trip_distance']
        input = self.spark.createDataFrame(data=inputData, schema=input_columns)

        expected_data = (("pickup_zoneA", "dropoff_zoneA", 3, 1),
                         ("pickup_zoneA", "dropoff_zoneB", 2, 2),
                         ("pickup_zoneA", "dropoff_zoneC", 2, 3),
                         ("pickup_zoneA", "dropoff_zoneD", 1, 4),
                         ("pickup_zoneA", "dropoff_zoneE", 1, 5),
                         ("pickup_zoneB", "dropoff_zoneA", 3, 1),
                         ("pickup_zoneB", "dropoff_zoneB", 2, 2),
                         ("pickup_zoneB", "dropoff_zoneC", 2, 3),
                         ("pickup_zoneB", "dropoff_zoneD", 1, 4),
                         ("pickup_zoneC", "dropoff_zoneE", 1, 1),
                         ("pickup_zoneC", "dropoff_zoneF", 1, 2)
                         )
        expected_columns = ['pickup_zone', 'dropoff_zone', 'number_of_dropoffs', 'rank']

        expected_output = self.spark.createDataFrame(data=expected_data, schema=expected_columns)
        # when

        actual_output = generate_insight_6_top_5_dropoffs_within_pickup_zone(input)

        # then
        self.assertTrue(are_dataframes_equal(actual_output, expected_output))

    def test_insight_7_average_trips_per_hour(self):
        """Test data pipeline.
        """
        # given
        inputData = (
            ("pickup_zoneA", "pickup_boroughZ", "dropoff_zoneA", "dropoff_boroughZ", 1, 1, '2015-01-19 0:54:16.0'),
            ("pickup_zoneA", "pickup_boroughZ", "dropoff_zoneA", "dropoff_boroughZ", 2, 2, '2015-01-20 0:54:16.0'),
            ("pickup_zoneA", "pickup_boroughZ", "dropoff_zoneB", "dropoff_boroughZ", 3, 3, '2015-01-22 0:54:16.0'),
            ("pickup_zoneA", "pickup_boroughY", "dropoff_zoneB", "dropoff_boroughZ", 4, 4, '2015-01-19 1:54:16.0'),
            ("pickup_zoneB", "pickup_boroughY", "dropoff_zoneC", "dropoff_boroughY", 5, 5, '2015-01-19 1:54:16.0'),
            ("pickup_zoneB", "pickup_boroughY", "dropoff_zoneD", "dropoff_boroughY", 6, 6, '2015-01-19 1:54:16.0'),
            ("pickup_zoneB", "pickup_boroughY", "dropoff_zoneD", "dropoff_boroughY", 7, 7, '2015-01-20 12:54:16.0'),
            ("pickup_zoneB", "pickup_boroughY", "dropoff_zoneD", "dropoff_boroughY", 8, 8, '2015-01-21 12:54:16.0'),
            ("pickup_zoneB", "pickup_boroughX", "dropoff_zoneD", "dropoff_boroughX", 9, 9, '2015-01-19 12:54:16.0'),
            ("pickup_zoneB", "pickup_boroughX", "dropoff_zoneD", "dropoff_boroughX", 10, 10, '2015-01-19 12:54:16.0')
        )

        input_columns = ['pickup_zone', 'pickup_borough', 'dropoff_zone', 'dropoff_borough', 'fare_amount',
                         'trip_distance',
                         'tpep_pickup_datetime']
        input = self.spark.createDataFrame(data=inputData, schema=input_columns)

        expected_data = (("00", 0.75),
                         ("01", 0.75),
                         ("12", 1.0)
                         )
        expected_columns = ['hour_of_day', 'average_trips']

        expected_output = self.spark.createDataFrame(data=expected_data, schema=expected_columns)
        # when

        actual_output = generate_insight_7_average_trips_per_hour(input)

        # then
        self.assertTrue(are_dataframes_equal(actual_output, expected_output))


def are_dataframes_equal(df_actual, df_expected):
    # sorts are needed in case if disordered columns
    a_cols = sorted(df_actual.columns)
    e_cols = sorted(df_expected.columns)
    # we don't know the column names so count on the first column we find
    df_a = df_actual.groupby(a_cols).agg(F.count(a_cols[1]))
    df_e = df_expected.groupby(e_cols).agg(F.count(e_cols[1]))
    # then perform our equality checks on the dataframes with the row counts
    if df_a.subtract(df_e).rdd.isEmpty():
        return df_e.subtract(df_a).rdd.isEmpty()
    return False


if __name__ == '__main__':
    unittest.main()
