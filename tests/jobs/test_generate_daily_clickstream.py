import pytest
from pyspark.sql import SparkSession, Row
from pyspark.testing import assertDataFrameEqual
from faker import Faker
from packages.faker_custom_providers import user_info
from jobs.daily_clickstream_generation import generate_clickstream_to_df


@pytest.fixture(scope = "package")
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Test Spark jobs") \
        .getOrCreate()
    
    yield spark

Faker.seed(0)

def test_generate_clickstream_to_df(spark: SparkSession):
    df_user_track_ids = spark.createDataFrame(
        [
            Row(user_id = "23749021asdf834", track_id = "412875addddd"),
            Row(user_id = "91asdf2857921835", track_id = "abcbad1412412"),
            Row(user_id = "2579adf18357", track_id = "412875abcbc"),
        ]
    )
    
    df = generate_clickstream_to_df(
        "2018-01-01", "2018-01-02", 
        df_user_track_ids, 2
    )

    assert df.count() == 2 * df_user_track_ids.count()