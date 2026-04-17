from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging
import argparse
from pyspark.sql.functions import max
from concurrent.futures import ThreadPoolExecutor
from src.util.nifi_metadata_util import generate_metadata, write_metadata
from src.util.log_util import sanitize_for_log

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")


def get_query(process_date, merch_region_cd):
    qry = f"""
    SELECT
         CAST(dw_merch_location_id AS BIGINT) AS dw_merch_location_id,
         CAST(pds1_paypass_acct_nbr_type_ind AS STRING) AS pds1_paypass_acct_nbr_type_ind,
         CAST(dw_issuer_id AS BIGINT) AS dw_issuer_id,
         'CORE' AS source_type,
         CAST(dw_merch_country_cd AS STRING) AS dw_merch_country_cd,
         CAST(dw_product_cd AS STRING) AS product_cd,
         CAST(local_txn_date AS STRING) AS local_txn_date,
         CAST(SUM(dw_net_pd_cnt) AS BIGINT) AS dw_net_pd_cnt,
         CAST(ROUND(SUM(dw_net_pd_amt), 4) AS DOUBLE) AS dw_net_pd_amt,
         CAST(dw_process_date AS STRING) AS process_date,
         CAST(dw_merch_region_cd AS STRING) AS dw_merch_region_cd
   FROM core.cut_clear_dtl_hsh_tbl
   WHERE dw_process_date = '{process_date}'
   AND de3_cardholder_txn_type in ('00', '09', '20') AND de4_txn_amt >= 1 AND dw_merch_region_cd = '{merch_region_cd}'
   GROUP BY
    dw_merch_location_id,
    pds1_paypass_acct_nbr_type_ind,
    dw_issuer_id,
    dw_merch_country_cd,
    dw_product_cd,
    local_txn_date,
    dw_process_date,
    dw_merch_region_cd
    union all
    SELECT
         CAST(dw_merch_location_id AS BIGINT) AS dw_merch_location_id,
         CAST(pds1_paypass_acct_nbr_type_ind AS STRING) AS pds1_paypass_acct_nbr_type_ind,
         CAST(de93_issuer_id AS BIGINT) AS dw_issuer_id,
         'GCO' AS source_type,
         CAST(dw_merch_country_cd AS STRING) AS dw_merch_country_cd,
         CAST(dw_product_cd AS STRING) AS product_cd,
         CAST(de12_txn_date AS STRING) AS local_txn_date,
         CAST(SUM(dw_net_pd_cnt) AS BIGINT) AS dw_net_pd_cnt,
         CAST(ROUND(SUM(dw_net_pd_amt), 4) AS DOUBLE) AS dw_net_pd_amt,
         CAST(dw_process_date AS STRING) AS process_date,
         CAST(dw_merch_region_cd AS STRING) AS dw_merch_region_cd
   FROM gco.clear_dtl_hsh_tbl
   WHERE dw_process_date = '{process_date}'
   AND de3_cardholder_txn_type in ('00', '09', '20') AND de4_txn_amt >= 1 AND dw_merch_region_cd = '{merch_region_cd}'
   GROUP BY
    dw_merch_location_id,
    pds1_paypass_acct_nbr_type_ind,
    de93_issuer_id,
    dw_merch_country_cd,
    dw_product_cd,
    local_txn_date,
    dw_process_date,
    dw_merch_region_cd
   """
    logger.info(qry)
    return qry


def get_latest_partition(spark, args, merch_region_cd):
    logger.info(f"Getting the dates for new load for {sanitize_for_log(merch_region_cd)}")
    partitions = spark.sql(f"""show partitions {args.tgt_table} partition(dw_merch_region_cd='{merch_region_cd}')""")\
        .select(max("partition")).collect()[0][0].split("=")[2]
    if len(partitions) > 0:
        return datetime.strptime(partitions, "%Y-%m-%d") + timedelta(1)


def drop_partition_if_exist(spark, args, dt, merch_region_cd):
    if spark.sql(
            f"""show tables in {args.tgt_table.split('.')[0]} like '{args.tgt_table.split('.')[-1]}'""").count() > 0:
        logger.info(
            f"alter table {args.tgt_table} drop if exists partition (dw_merch_region_cd='{merch_region_cd}', process_date='{dt}')")
        spark.sql(
            f"alter table {args.tgt_table} drop if exists partition (dw_merch_region_cd='{merch_region_cd}', process_date='{dt}')")
    else:
        logger.info(f"Partition cannot be dropped as table {args.tgt_table} not found")


def create_sme_clearing_table(spark, args):
    tbl_qry = f"""
    CREATE TABLE IF NOT EXISTS {args.tgt_table} (
        dw_merch_location_id BIGINT,
        pds1_paypass_acct_nbr_type_ind STRING,
        dw_issuer_id BIGINT,
        source_type string,
        dw_merch_country_cd STRING,
        product_cd STRING,
        local_txn_date STRING,
        dw_net_pd_cnt BIGINT,
        dw_net_pd_amt DOUBLE,
        process_date STRING,
        dw_merch_region_cd STRING
    )
    PARTITIONED BY (dw_merch_region_cd, process_date)
    STORED AS PARQUET
    """
    spark.sql(tbl_qry)


def process_data(spark, args, dt, merch_region_cd, meta_data_lst):
    df = spark.sql(get_query(dt, merch_region_cd))
    if (not df.isEmpty()):
        # creating the nifi meta data values
        tbl_name = f"{args.tgt_table.split('.')[-1]}_{dt}"
        hdfsDirectoryPath = f"{args.root_path}/{args.tgt_table.split('.')[-1]}/dw_merch_region_cd={merch_region_cd}/process_date={dt}"
        s3Bucket = f"{args.s3Bucket}"
        s3prefix = f"{args.s3prefix}/{args.tgt_table.split('.')[-1]}/dw_merch_region_cd={merch_region_cd}/process_date={dt}"
        hdfs_file_count = df.rdd.getNumPartitions() + 1
        df.write.mode("overwrite").parquet(
            f"{args.root_path}/{args.tgt_table.split('.')[-1]}/dw_merch_region_cd={merch_region_cd}/process_date={dt}")
        logger.info(
            f"Data written to the path {args.root_path}/{args.tgt_table.split('.')[-1]}/dw_merch_region_cd={merch_region_cd}/process_date={dt}")
        meta_data_lst.append(generate_metadata(tbl_name, hdfsDirectoryPath, s3Bucket, s3prefix, hdfs_file_count))
        logger.info(f"metadata created successfully for {dt} and {merch_region_cd}")


def run(spark, args):
    data = []
    dt_format = '%Y-%m-%d'
    create_sme_clearing_table(spark, args)
    for region in args.merch_region_cd.replace(' ', '').split(","):
        start_date = datetime.strptime(args.start_date, dt_format) if args.start_date != "None" else get_latest_partition(
            spark, args, region)
        end_date = datetime.strptime(args.end_date, dt_format) if args.end_date != "None" else datetime.today()
        logger.info(f"Processing start_date is {sanitize_for_log(start_date)} and end_date is {sanitize_for_log(end_date)} for region {sanitize_for_log(region)}")
        days_pending = end_date - start_date
        logger.info(f"pending days..{sanitize_for_log(days_pending)}")
        partitions_days = []
        for i in range(0, days_pending.days + 1):
            partitions_days.append(datetime.strftime(start_date + timedelta(i), dt_format))
        with ThreadPoolExecutor(args.max_worker) as executor:
            futures = [executor.submit(process_data, spark, args, dt, region, data) for dt in partitions_days]
        for future in futures:
            future.result()
    if (len(data) > 0):
        logger.info(f"MSCK REPAIR TABLE {sanitize_for_log(args.tgt_table)}")
        spark.sql(f"MSCK REPAIR TABLE {args.tgt_table}")
        file_name = f"{args.tgt_table.split('.')[-1]}_{datetime.today().strftime('%Y%m%d%H%M%S')}.json"
        file_system = args.root_path
        write_metadata(spark, file_system, args.metadata_path, file_name, data)


def parse_args():  # pragma no cover
    parser = argparse.ArgumentParser(description="online cri repeating attr")
    parser.add_argument("--tgt_table", type=str, required=False, help="target table name")
    parser.add_argument('--start_date', type=str, required=True, help="start date")
    parser.add_argument('--end_date', type=str, required=True, help="end date")
    parser.add_argument('--merch_region_cd', type=str, required=True, help="merch_region_cd")
    parser.add_argument('--metadata_path', type=str, required=True, help="path of the metadata")
    parser.add_argument('--root_path', type=str, required=True, help="root path")
    parser.add_argument('--s3Bucket', type=str, required=True, help="bucket name")
    parser.add_argument('--s3prefix', type=str, required=True, help="s3 prefix")
    parser.add_argument('--max-worker', type=int, required=False, default=10)
    return parser.parse_args()


if __name__ == '__main__':  # pragma no cover
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    args = parse_args()
    print(args)
    run(spark, args)
