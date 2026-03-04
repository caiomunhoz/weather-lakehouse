from pyspark import pipelines as dp
from utilities.schemas import RAW_SCHEMA

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