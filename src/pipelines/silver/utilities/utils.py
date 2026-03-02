from pyspark.sql import functions as F

VALID_WEATHER = {
    'valid_latitude': 'latitude BETWEEN -90 AND 90',
    'valid_longitude': 'longitude BETWEEN -180 AND 180',
    'valid_time': 'time IS NOT NULL',
    'valid_temperature': 'temperature BETWEEN -100 AND 70',
    'valid_precipitation': 'precipitation >= 0',
    'valid_apparent_temperature': 'apparent_temperature BETWEEN -120 AND 80',
    'valid_cloud_cover': 'cloud_cover BETWEEN 0 AND 100'
}

def explode_hourly(df):
    return df.withColumn(
        'hourly_exploded',
        F.explode(F.arrays_zip(
            F.col('hourly.time'),
            F.col('hourly.temperature_2m'),
            F.col('hourly.precipitation'),
            F.col('hourly.apparent_temperature'),
            F.col('hourly.cloud_cover')
        ))
    )