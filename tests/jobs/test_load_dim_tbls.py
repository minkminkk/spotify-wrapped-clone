import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.testing import assertDataFrameEqual

from load_dim_tbls import get_df_tracks, generate_df_users, generate_df_dates
from datetime import date, timedelta


@pytest.fixture(scope = "package", autouse = True)
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Test Spark jobs") \
        .getOrCreate()
    
    yield spark


def test_get_df_tracks(spark):
    pass


def test_generate_df_users(spark):
    df = generate_df_users(no_users = 10)

    # Test output DataFrame schema
    expected_cols = (
        *("user_dim_id", "user_id", "username", "name"), 
        *("sex", "address", "email", "birth_date")
    )
    assert set(df.columns) == set(expected_cols)

    # Assert columns in DataFrame
    assertDataFrameEqual(
        df.select("user_dim_id"),
        spark.range(1, df.count() + 1).withColumnRenamed("id", "user_dim_id")
    )   # only need to test user_dim_id values because other fields
        # are included as part of faker_custom_providers package


def test_get_df_dates(spark):
    start_date = date.fromisoformat("2018-01-01")
    end_date = date.fromisoformat("2018-01-05")
    num_days = (end_date - start_date).days

    def dt_to_date_dict(d: date):
        return {
            "date_dim_id": d.year * 10000 + d.month * 100 + d.day,
            "full_date": d,
            "year": d.year,
            "month": d.month,
            "day": d.day,
            "week_of_year": d.isocalendar().week,
            "day_of_week": d.weekday() + 2
                # Spark weekday Monday = 2 while Monday = 0 for date
        }

    df = generate_df_dates(start_date.isoformat(), end_date.isoformat())
    schema = StructType(
        [
            StructField("date_dim_id", IntegerType()),
            StructField("full_date", DateType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("day", IntegerType()),
            StructField("week_of_year", IntegerType()),
            StructField("day_of_week", IntegerType()),
        ]
    )   # By default, Python int -> LongType while Spark SQL use IntegerType 

    # Test output DataFrame
    expected_df = spark.createDataFrame(
        [
            dt_to_date_dict(start_date + timedelta(days = days)) \
                for days in range(num_days + 1) # end_date inclusive
        ],
        schema = schema
    )

    assertDataFrameEqual(df, expected_df)
    
