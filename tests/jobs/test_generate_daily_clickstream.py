import pytest

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
from faker import Faker

from jobs.daily_clickstream_generate import generate_clickstream_to_df


@pytest.fixture(scope = "package", autouse = True)
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("Test Spark jobs") \
        .getOrCreate()
    
    yield spark

@pytest.fixture(scope = "module")
def fake():
    fake = Faker()
    Faker.seed(0)

    return fake


def test_generate_clickstream_to_df(fake: Faker):
    # Inputs
    str_template = "^" * 22
    map_user_track_ids = [
        Row(
            user_id = fake.hexify(str_template), 
            track_ids = [fake.hexify(str_template) for _ in range(5)]
        ) for _ in range(5)
    ]
    max_events_per_user = 2
    
    # Call function
    df = generate_clickstream_to_df(
        "2018-01-01", "2018-01-02", 
        map_user_track_ids, max_events_per_user
    )

    # Expected results
    expected_schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("event_name", StringType()),
            StructField("ipv4", StringType()),
            StructField("user_id", StringType()),
            StructField("user_agent", StringType()),
            StructField("track_id", StringType())
        ]
    )
    expected_length = max_events_per_user * len(map_user_track_ids)

    # Test output
    assertSchemaEqual(df.schema, expected_schema)
    assert df.count() == expected_length