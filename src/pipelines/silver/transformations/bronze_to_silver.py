from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.utils import explode_hourly

BRONZE_PATH = spark.conf.get('bronze_path')

@dp.table(
    table_properties={'quality': 'silver'}
)
def observations():
    return (
        spark.read
        .table(f'{BRONZE_PATH}.observations')
        .transform(explode_hourly)
        .select(
            'latitude',
            'longitude',
            'timezone',
            F.to_timestamp(F.col('hourly_exploded.time'), "yyyy-MM-dd'T'HH:mm").alias('time'),
            F.col('hourly_exploded.temperature_2m').alias('temperature'),
            F.col('hourly_exploded.precipitation').alias('precipitation'),
            F.col('hourly_exploded.apparent_temperature').alias('apparent_temperature'),
            F.col('hourly_exploded.cloud_cover').alias('cloud_cover')
        )
    )

@dp.table(
    table_properties={'quality': 'silver'}
)
def forecasts():
    return (
        spark.read
        .table(f'{BRONZE_PATH}.forecasts')
        .transform(explode_hourly)
        .select(
            'latitude',
            'longitude',
            'timezone',
            F.to_timestamp(F.col('hourly_exploded.time'), "yyyy-MM-dd'T'HH:mm").alias('time'),
            F.col('hourly_exploded.temperature_2m').alias('temperature'),
            F.col('hourly_exploded.precipitation').alias('precipitation'),
            F.col('hourly_exploded.apparent_temperature').alias('apparent_temperature'),
            F.col('hourly_exploded.cloud_cover').alias('cloud_cover')
        )
    )

