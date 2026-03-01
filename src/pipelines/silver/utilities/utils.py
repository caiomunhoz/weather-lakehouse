from pyspark.sql import functions as F

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