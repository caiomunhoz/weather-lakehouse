from pyspark.sql import functions as F

def hash_coordinates(df):
    return df.withColumn(
        'location_id',
        F.hash(F.concat('latitude', 'longitude'))
    )