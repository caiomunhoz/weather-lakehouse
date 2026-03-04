from pyspark import pipelines as dlt
from pyspark.sql import functions as F
from utilities.utils import hash_coordinates

SILVER_PATH = spark.conf.get('silver_path')

@dlt.table(
    table_properties={'quality': 'gold'}
)
def weather_time_series():
    forecasts = (
        spark.read.table(f'{SILVER_PATH}.forecasts')
        .withColumn('source_type', F.lit('forecast'))
    )

    observations = (
        spark.read.table(f'{SILVER_PATH}.observations')
        .withColumn('source_type', F.lit('observation'))
    )

    return (
        forecasts.union(observations)
        .transform(hash_coordinates)
    )

