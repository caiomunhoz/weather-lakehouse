from pyspark import pipelines as dlt
from pyspark.sql import functions as F
from utilities.utils import hash_coordinates

SILVER_PATH = spark.conf.get('silver_path')
GOLD_PATH = spark.conf.get('gold_path')

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

@dlt.table(
    table_properties={'quality': 'gold'}
)
def weather_daily_summary():
    observations = (
        spark.read
        .table(f'{GOLD_PATH}.weather_time_series')
        .filter("source_type = 'observation'")
    )

    return (
        observations
        .withColumn('date', F.to_date('time'))
        .groupBy('location_id', 'date')
        .agg(
            F.expr('ROUND(AVG(temperature), 2)').alias('avg_temperature'),
            F.max('temperature').alias('max_temperature'),
            F.min('temperature').alias('min_temperature'),
            F.expr('ROUND(AVG(apparent_temperature), 2)').alias('avg_apparent_temperature'),
            F.expr('ROUND(SUM(precipitation), 2)').alias('total_precipitation'),
            F.expr('ROUND(AVG(cloud_cover), 2)').alias('avg_cloud_cover')
        )
    )

@dlt.table(
    table_properties={'quality': 'gold'}
)
def forecasts_vs_observations():
    forecasts = spark.read.table(f'{SILVER_PATH}.forecasts').alias('f')
    observations = spark.read.table(f'{SILVER_PATH}.observations').alias('o')

    return (
        forecasts
        .join(
            observations, 
            ['latitude', 'longitude', 'time', 'timezone']
        )
        .transform(hash_coordinates)
        .select(
            'location_id',
            'latitude',
            'longitude',
            'time',
            'timezone',
            F.col('o.temperature').alias('observed_temp'),
            F.col('f.temperature').alias('forecasted_temp'),
            F.expr(
                'ROUND(ABS(forecasted_temp - observed_temp), 2)'
            ).alias('forecast_temp_error'),
            F.col('o.apparent_temperature').alias('observed_apparent_temp'),
            F.col('f.apparent_temperature').alias('forecasted_apparent_temp'),
            F.expr(
                'ROUND(ABS(forecasted_apparent_temp - observed_apparent_temp), 2)'
            ).alias('forecast_apparent_temp_error'),
            F.col('o.precipitation').alias('observed_precipitation'),
            F.col('f.precipitation').alias('forecasted_precipitation'),
            F.expr(
                'ROUND(ABS(forecasted_precipitation - observed_precipitation), 2)'
            ).alias('forecast_precipitation_error'),
            F.col('o.cloud_cover').alias('observed_cloud_cover'),
            F.col('f.cloud_cover').alias('forecasted_cloud_cover'),
            F.expr(
                'ABS(forecasted_cloud_cover - observed_cloud_cover)'
            ).alias('forecast_cloud_cover_error')
        )
    )