from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType, StructField, DoubleType, 
    IntegerType, StringType, ArrayType
)

RAW_SCHEMA = StructType([
    StructField('latitude', DoubleType(), False),
    StructField('longitude', DoubleType(), False),
    StructField('generationtime_ms', DoubleType(), False),
    StructField('utc_offset_seconds', IntegerType(), False),
    StructField('timezone', StringType(), False),
    StructField('timezone_abbreviation', StringType(), False),
    StructField('elevation', DoubleType(), False),
    StructField(
        'hourly_units',
        StructType([
            StructField('time', StringType(), False),
            StructField('temperature_2m', StringType(), False),
            StructField('precipitation', StringType(), False),
            StructField('apparent_temperature', StringType(), False),
            StructField('cloud_cover', StringType(), False)
        ]),
        False
    ),
    StructField(
        'hourly',
        StructType([
            StructField('time', ArrayType(StringType()), False),
            StructField('temperature_2m', ArrayType(DoubleType()), False),
            StructField('precipitation', ArrayType(DoubleType()), False),
            StructField('apparent_temperature', ArrayType(DoubleType()), False),
            StructField('cloud_cover', ArrayType(IntegerType()), False)
        ]),
        False
    )
])

LANDING_PATH = spark.conf.get('landing_path')

@dp.table(
    table_properties={'quality': 'bronze'}
)
def observations():
    return (
        spark.readStream
        .format('cloudFiles')
        .schema(RAW_SCHEMA)
        .option('cloudFiles.format', 'json')
        .option('multiLine', True)
        .load(f'{LANDING_PATH}/observations')
    )

@dp.table(
    table_properties={'quality': 'bronze'}
)
def forecasts():
    return (
        spark.readStream
        .format('cloudFiles')
        .schema(RAW_SCHEMA)
        .option('cloudFiles.format', 'json')
        .option('multiLine', True)
        .load(f'{LANDING_PATH}/forecasts')
    )