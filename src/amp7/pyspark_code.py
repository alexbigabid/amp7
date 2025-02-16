import boto3

from argparse import ArgumentParser
from typing import List, Callable
from datetime import datetime, timezone
from enum import Enum
from urllib.parse import urlparse

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType

class AggType(Enum):
    STRUCT_JSON = 'struct_json'
    COLUMNAR = 'columnar'

def spark_init() -> SparkSession:
    spark = (SparkSession
        .builder
        .appName("amp7")
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("parquet.enable.summary-metadata", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark

def filter_publisher_type(df: DataFrame) -> DataFrame:
    return df.filter(
        F.coalesce(F.lower(F.col('publisher_type')), F.lit('app')) == 'app'
    )

def ifa_filters(df: DataFrame, ifa_column: str = 'ifa') -> DataFrame:
    res_df = df.filter((F.col(ifa_column).isNotNull())
                       & (F.length(F.col(ifa_column)) == 32)
                       & (F.regexp_replace(F.col(ifa_column), "0", "") != "")
                       )
    return res_df


def filter_exchanges(df: DataFrame) -> DataFrame:
    # TODO: filter exchanges from config like in Upsolver
    exchanges = {"ironsource", "vungle", "unity", "applovin", "fyber", "chartboost", 'adx'}
    return df.filter(F.col('exchange').isin(exchanges))

def enrich(df: DataFrame) -> DataFrame:
    return (df
            .withColumn('activity', F.concat_ws('__', F.col('activity'), F.col('traffic_type')))
            .withColumn('adunit', F.concat_ws('__', 'ad_placement', 'ad_format_type'))
            .withColumn(
                "trunc_day_request_unixtime", ((F.floor(F.col('request_unixtime')/86400)) * 86400).cast('long').alias('trunc_day_request_unixtime'))
            .drop('ad_placement', 'ad_format_type', 'partition_date', 'traffic_type')
            )

def agg_to_json(input_df: DataFrame, group_cols: List[str], metric_cols: List[str], metric_name: str, agg_func: Callable=None) -> DataFrame:
    if agg_func:
        if agg_func.__name__ in ['max_by', 'min_by']:
            # max_by/min_by implemented with window function.
            # Added handling for cases when the expected value has multiple rows for the same aggregation keys, so it will return the first non null value (if exits) when using max_by/min_by.
            # for example: 
            # pm_id, timestamp, ifa
            # 1, 100, aaa
            # 1, 100, 
            # query for some base col: SELECT pm_id, MAX_BY(ifa, timestamp) FROM df GROUP BY 1
            # when using max_by/min_by functions on this case without oredering- it will be volatile because it will return a random `ifa` value.
            window_spec = Window.partitionBy(group_cols).orderBy(*[F.desc_nulls_last(c) for c in metric_cols[::-1]]) # using reversed list (on `metric_cols`) because we want to sort first by the max value and then by its value associated with the maximum value of order so it will eliminate nulls on the associated values.
            agg_df = (input_df
                        .withColumn('rn', F.row_number().over(window_spec))
                        .withColumnRenamed(metric_cols[0], f"agg_{metric_cols}")
                        .filter(F.col('rn') == 1)
                        .drop('rn')
                        .drop(metric_cols[-1])
            )
        else:
            agg_df = input_df.groupBy(group_cols).agg(agg_func(*metric_cols).alias(f"agg_{metric_cols}"))
        collect_method = F.collect_set
    
    elif not(agg_func):
        agg_df = input_df.withColumnRenamed(metric_cols[0], f"agg_{metric_cols}")
        collect_method = F.collect_list # Because we cannot use `collect_set` when aggregating `MAP` type field.
        
    for i in range(1, len(group_cols)):

        value_col = f"agg_{metric_cols}" if i == 1 else f"agg_per_{group_cols[-i+1]}"
        agg_col_alias = metric_name if i == len(group_cols)-1 else f"agg_per_{group_cols[-i]}"
        agg_df = (agg_df
                        .groupBy(group_cols[:-i])
                        .agg(F.to_json(
                            F.map_from_entries(
                                collect_method(
                                    F.when(F.col(group_cols[-i]).isNotNull(),
                                        F.struct(group_cols[-i], value_col)
                                        )
                                    )
                                )
                            ).alias(agg_col_alias)
                        )
                    )
    return agg_df

def save_sliding_window_to_s3(df: DataFrame, target_bucket: str, target_prefix: str, partition_columns: str, output_table: str, execution_date: str, amount_of_partitions: int = None) -> None:
    '''
    Save the DataFrame to S3
    df: DataFrame
    target_bucket: str
    target_prefix: str
    partition_columns: str
    '''
    if amount_of_partitions:
        df = df.repartition(amount_of_partitions)
    print(f"Saving the DataFrame to S3: {target_bucket}/{target_prefix}")
    (df
        .write
        .partitionBy(partition_columns) \
        .mode('overwrite')
        .format('parquet')
        .save(f's3://{target_bucket}/{target_prefix}')
    )

    add_glue_partition(f's3://{target_bucket}/{target_prefix}', output_table, execution_date)


    
def add_partition_columns(df: DataFrame, execution_date: str) -> DataFrame:
    '''
    Add partitions columns to the DataFrame
    df: DataFrame
    execution_date: str
    return: DataFrame
    '''

    parquet_output_df = df \
       .withColumn("year", F.year(F.lit(execution_date))) \
       .withColumn("month", F.format_string("%02d", F.month(F.lit(execution_date)))) \
       .withColumn("day", F.format_string("%02d", F.dayofmonth(F.lit(execution_date)))) \
       .withColumn("hour", F.format_string("%02d", F.hour(F.lit(execution_date)))) \
       .withColumn("minute", F.format_string("%02d", F.minute(F.lit(execution_date))))
    
    return parquet_output_df

def add_partition_string_from_execution_date(execution_date: str) -> str:
    '''
    Add partition string from the execution date
    execution_date: str
    return: str
    '''
    return f"year={execution_date[:4]}/month={execution_date[5:7]}/day={execution_date[8:10]}/hour={execution_date[11:13]}/minute={execution_date[14:16]}"

def remove_json_escaping(json_col):
    res = F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(json_col,
                                    '\\\\', ''),
                        '\"\{', '{'),
                    '\}\"', '}')
    return res

def set_ttl(df: DataFrame, interval_start: int, 
                target_col: str='max_timestamp', 
                group_by: list=['ifa'], order_by: list=['ifa'], 
                relative_to__timestamp: int=int(datetime.now(timezone.utc).timestamp()), 
                compute_full_window: bool=False, interval_min:int = 15) -> DataFrame:
    
        unix_7_days = 60*60*24*7 + (interval_min+2)*60.0 # 1 minute*minutes_in_hour * hours*days + (8+2) hours (**8 hours interval and 2 hours of data ingest (from exchange level json to Aerospuke) to hold the ifa's until the next interval load**)

        assert interval_start < relative_to__timestamp, \
            '`interval_start` given is bigger than `relative_to__timestamp` given to calculate ttl'

        window_spec = Window.partitionBy(*group_by).orderBy(*order_by)
        
        df = df.withColumn('__max_timestamp', F.max(F.col(target_col)).over(window_spec)) \
                .withColumn('__current_unix_timestamp', F.lit(relative_to__timestamp)) \
                .withColumn('interval_start', F.lit(interval_start)) \
                .withColumn('__ttl',
                            (unix_7_days - (F.col('__current_unix_timestamp') - F.col('__max_timestamp')))
                        )

        ttl_on_full_window_case = F.col('__ttl') if compute_full_window else F.lit(-2)
        
        return df \
                .withColumn('ttl',
                    F.when( # ifa not in current interval
                        (F.col('__max_timestamp') < F.col('interval_start'))
                        & (F.col('__ttl') > 0), # Avioiding __ttl IN (0, -1, -2) https://docs.aerospike.com/server/operations/configure/namespace/retention#:~:text=A%20TTL%20of%200%20instructs
                            ttl_on_full_window_case)
                    
                    .when( # ifa in current interval
                        (F.col('__max_timestamp').between(F.col('interval_start'), F.col('__current_unix_timestamp')))
                        & (F.col('__ttl') > 0), # Avioiding __ttl IN (0, -1, -2) https://docs.aerospike.com/server/operations/configure/namespace/retention#:~:text=A%20TTL%20of%200%20instructs
                        F.col('__ttl'))
                    .otherwise(F.lit(None)).cast("long")
                ) \
                .drop('__max_timestamp', '__ttl', 'interval_start', '__current_unix_timestamp') \
                .filter(F.col('ttl').isNotNull())

def get_json_aggregated_df(spark, df: DataFrame, metric_configuration: dict) -> DataFrame:
    metrics_df = spark.createDataFrame([], StructType([]))
    for metric_name, configuration in metric_configuration.items():
        agg_type = configuration['agg_type']
        group_cols = configuration["group_cols"]
        metric_cols = configuration["metric_cols"]
        agg_func = configuration["agg_func"]

        if agg_type==AggType.COLUMNAR.value:
            result_df = df.groupBy(*group_cols).agg(agg_func(F.col(metric_cols[0])).alias(metric_name))
        
        elif agg_type==AggType.STRUCT_JSON.value:
            result_df = agg_to_json(df, group_cols, metric_cols, metric_name, agg_func)
        else:
            raise Exception(f"Expected aggergation type: {[atype.value for atype in AggType.__members__.values()]} for the metric: {metric_name}. got: {agg_type}")

        metrics_df = metrics_df.unionByName(result_df, allowMissingColumns=True)

    window_spec = Window.partitionBy("ifa").orderBy("ifa")

    for metric in metric_configuration.keys():
        metrics_df = metrics_df.withColumn(metric, F.first(metric, ignorenulls=True).over(window_spec))

    res_df = (metrics_df
              .withColumn('rr', F.row_number().over(window_spec))
              .filter(F.col('rr') == 1)
              .drop('rr'))
    
    return res_df


def get_aggregated_df(df):
    group_columns = ['ifa', 'exchange', 'adunit', 'activity', 'app_id', 'trunc_day_request_unixtime']
    agg_df = (
        df
        .groupBy(*group_columns)
        .agg(
            F.count("*").alias("impressions__count_impression_7d_per_exch_adunit_activity_appid_impression_date"),
            F.max('request_unixtime').alias('max_request_unixtime')
            )
        )
    
    return agg_df

def add_glue_partition(s3_path, table_name, execition_date):
    # Initialize the Glue client
    glue_client = boto3.client('glue', region_name='us-east-1')

    database_name = table_name.split('.')[0]
    table_name = table_name.split('.')[1]

    s3_path = s3_path + '/' + add_partition_string_from_execution_date(execition_date)

    # Parse the S3 path to extract partition values
    parsed_url = urlparse(s3_path)
    path_parts = parsed_url.path.strip('/').split('/')
    partition_values = {}
    for part in path_parts:
        if '=' in part:
            key, value = part.split('=')
            partition_values[key] = value

    # Ensure all required partition keys are present
    required_keys = ['year', 'month', 'day', 'hour', 'minute']
    if not all(key in partition_values for key in required_keys):
        raise ValueError(f"Missing one or more required partition keys: {required_keys}")

    # Retrieve the existing table's storage descriptor
    table = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    storage_descriptor = table['Table']['StorageDescriptor']

    # Update the storage descriptor's location to point to the new partition's data
    new_s3_location = s3_path
    storage_descriptor['Location'] = new_s3_location

    # Create the partition with the extracted values
    partition_input = {
        'Values': [partition_values[key] for key in required_keys],
        'StorageDescriptor': storage_descriptor
    }

    try:
        response = glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input
        )
        print(f"Partition added successfully: {response}")
    except glue_client.exceptions.AlreadyExistsException:
        print("Partition already exists.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main(execution_datetime: str, target_bucket: str, target_prefix: str, sliding_window_table:str, feature_by_ifa_table:str) -> None:
    # execution_date = '2025-01-05T07:50:00'
    end_ts = int(datetime.strptime(execution_datetime, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
    seven_days_ago = end_ts - (7 * 24 * 60 * 60)
    output_partition_columns = ['year', 'month', 'day', 'hour', 'minute']

    spark = spark_init()
    imp_df = spark.table("live_funnel.parquet_impressions")

    imp_df = imp_df.select(
        "ifa",
        "exchange",
        "ad_placement",
        "ad_format_type",
        "app_id",
        "activity",
        "traffic_type",
        "request_unixtime",
        "publisher_type",
        "os",
        "partition_date"
    )

    # Filter rows where unixtime is within the last 7 days
    filtered_df = imp_df.filter((F.col("request_unixtime").between(seven_days_ago, end_ts)))

    filtered_df = filter_publisher_type(filtered_df)
    filtered_df = ifa_filters(filtered_df)
    filtered_df = filter_exchanges(filtered_df)
    enriched_df = enrich(filtered_df)

    # Aggregating the data
    agg_df = get_aggregated_df(enriched_df)
    
    # Setting TTL to 7 days
    agg_df = set_ttl(
        df=agg_df,
        interval_start=seven_days_ago,
        target_col='max_request_unixtime',
        relative_to__timestamp=end_ts,
        interval_min=15)
    # Writing the aggregated data to S3
    agg_output_df = add_partition_columns(agg_df, execution_datetime)
    agg_output_df.cache()
    save_sliding_window_to_s3(
        agg_output_df, target_bucket, f'{target_prefix}/sliding_window',
        output_partition_columns, sliding_window_table, execution_datetime, amount_of_partitions=4)


    # Aggregating the data to JSON
    metric_configuration = {
        "impressions__count_impression_7d_per_exch_adunit_activity_appid_impression_date": {
            "group_cols": ['ifa', 'exchange', 'adunit', 'activity', 'app_id', 'trunc_day_request_unixtime'],
            "metric_cols": ["impressions__count_impression_7d_per_exch_adunit_activity_appid_impression_date"],
            "agg_func": F.sum,
            "agg_type": "struct_json",
        },
        "ttl": {
            "group_cols": ["ifa"],
            "metric_cols": ["ttl"],
            "agg_func": F.max,
            "agg_type": "columnar",
            }
    }

    agg_json_output_df = get_json_aggregated_df(spark, agg_output_df, metric_configuration)
    # Writing the aggregated JSON data to S3
    updated_at_col = F.lit(int(end_ts) * 1_000).cast('long')

    res_df = agg_json_output_df.select(
         "ifa",
         "ttl",
         F.to_json(
             F.struct(
                 "impressions__count_impression_7d_per_exch_adunit_activity_appid_impression_date",
                 updated_at_col.alias('updated_at')
             )
         ).alias('amp_imp_data')
     ).withColumn('amp_imp_data', remove_json_escaping('amp_imp_data'))
    
    res_df = add_partition_columns(res_df, execution_datetime)
    save_sliding_window_to_s3(res_df, target_bucket, f'{target_prefix}/sliding_window_base_columns', 
                              output_partition_columns, feature_by_ifa_table, execution_datetime, amount_of_partitions=4)

    spark.stop()


if __name__ == "__main__":
    # Read CL arguments
    argparse = ArgumentParser()
    argparse.add_argument('--execution_datetime', default='', type=str, help='Specific date to run in yyyy-mm-dd format')
    argparse.add_argument('--target_bucket', default='', type=str, help='The target S3 bucket to which the results will be written')
    argparse.add_argument('--target_prefix', type=str, required=True, help='The target S3 prefix to which the results will be written') 
    argparse.add_argument('--sliding_window_table', type=str, required=True, help='The target table to which the sliding window data will be written')
    argparse.add_argument('--feature_by_ifa_table', type=str, required=True, help='The target table to which the feature by ifa data will be written')
    
    args = argparse.parse_args()

    execution_datetime = args.execution_datetime
    target_bucket = args.target_bucket
    target_prefix = args.target_prefix
    sliding_window_table = args.sliding_window_table
    feature_by_ifa_table = args.feature_by_ifa_table
    
    main(execution_datetime, target_bucket, target_prefix, sliding_window_table, feature_by_ifa_table)