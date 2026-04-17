import argparse
import sys
import traceback
from argparse import Namespace
from datetime import datetime
from typing import List
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, when


# To ensure all required columns are present in the final output table
required_cols = ['file_id', 'file_original_name', 'entity_id', 'supplier_id', 'supplier_name', 'street_address', 'city', 'state', 'postal_code',
                 'country', 'phone_number', 'email_address', 'payment_method', 'payment_terms', 'currency', 'total_number_of_invoices',
                 'total_amount_of_invoices', 'total_number_of_purchase_orders', 'total_amount_of_purchase_orders', 'total_number_of_payments_paid',
                 'total_amount_of_payments_paid', 'total_number_of_payments_due', 'total_amount_of_payments_due', 'total_number_of_payments_open',
                 'total_amount_of_payments_open', 'merchant_id', 'annual_target_spend', 'transaction_count', 'average_ticket',
                 'total_spend', 'total_spent_by_card', 'total_spent_by_eft', 'total_spent_by_cheque', 'total_spent_by_ach', 'total_spent_by_other',
                 'issuer_id', 'payment_term_days', 'mapped_payment_method', 'all_suppliers_total_number_of_payment',
                 'all_suppliers_total_number_of_suppliers', 'all_suppliers_total_number_of_invoices', 'all_suppliers_total_spend',
                 'all_supplier_total_spent_by_card', 'all_supplier_total_spent_by_eft', 'all_supplier_total_spent_by_cheque',
                 'all_supplier_total_spent_by_ach', 'all_supplier_total_spent_by_other', 'all_suppliers_total_card_accepting_suppliers',
                 'next_payment_date', 'is_matched', 'supplier_website_url', 'region', 'mcc_code', 'supplier_alias_name',
                 'industry_classification_code', 'preferred_payment_method', 'propensity_score', 'transaction_recency', 'sic',
                 'suggested_talking_points', 'cleansed_supplier_name', 'cleansed_address_line_1', 'cleansed_city_name',
                 'Cleansed_state_Or_province_name', 'cleansed_country_code', 'cleansed_postal_code', 'aggregate_merchant_name',
                 'aggregate_merchant_id', 'parent_aggregate_merchant_name', 'parent_aggregate_merchant_id', 'mcc_group', 'business_region_name',
                 'commercial_history', 'commercial_recency', 'clearing_last_seen_date', 'auth_last_seen_date', 'avg_tran_amt', 'matched_trans_count',
                 'in_control_history', 'in_control_recency', 'last_update_date', 'customer_naics', 'mmh_id', 'card_acceptor',
                 'propensity_score_label', 'dw_merch_region_cd', 'confidence', 'industry']


def _get_dbutils():
    """getting db utils session"""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        return DBUtils(SparkSession.getActiveSession())
    except Exception:
        return None


def _set_task_value(key: str, value: str) -> None:
    """setting task value"""
    dbutils = _get_dbutils()
    try:
        if dbutils:
            dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore
    except Exception:
        pass


def build_ap_query(args: Namespace) -> str:
    """building ap query for fetching data"""
    return f"""
    WITH ap_data AS (
    SELECT ap.supplier_id,
            ap.supplier_name,
            ap.street_address,
            ap.city,
            ap.state,
            ap.postal_code,
            "USA" AS country,
            ap.phone_number,
            ap.email_address,
            ap.payment_terms,
            ap.payment_method,
            ap.currency,
            ap.total_number_of_invoices,
            ap.total_amount_of_invoices,
            ap.total_number_of_purchase_orders,
            ap.total_amount_of_purchase_orders,
            ap.total_number_of_payments_paid,
            ap.total_amount_of_payments_paid,
            ap.total_number_of_payments_due,
            ap.total_amount_of_payments_due,
            ap.total_number_of_payments_open,
            ap.total_amount_of_payments_open,
            ap.merchant_id,
            ap.annual_target_spend,
            ap.transaction_count,
            ap.file_id,
            ap.file_original_name,
            ap.issuer_id,
            ap.ingestion_ts,
           ptm.days AS payment_term_days,
           pm.sme_payment_method AS mapped_payment_method
    FROM {args.ap_raw_tbl} ap
    LEFT JOIN {args.ptm_tbl} ptm
        ON LOWER(ap.payment_terms) = LOWER(ptm.payment_term)
    LEFT JOIN {args.pm_tbl} pm
        ON LOWER(ap.payment_method) = LOWER(pm.src_payment_method)
    WHERE file_id = '{args.file_id}'
    ),
    ap_row_contrib AS (
        SELECT
            *,
            CASE
                WHEN transaction_count = 0 THEN NULL
                ELSE annual_target_spend / transaction_count
            END AS average_ticket,
            COALESCE(total_number_of_payments_paid, 0)
          + COALESCE(total_number_of_payments_due, 0)
          + COALESCE(total_number_of_payments_open, 0) AS c_total_number_of_payment,
            COALESCE(total_number_of_invoices, 0) AS c_total_number_of_invoices,
            COALESCE(total_amount_of_payments_paid, 0.0)
          + COALESCE(total_amount_of_payments_due, 0.0)
          + COALESCE(total_amount_of_payments_open, 0.0) AS total_spend,
            CASE WHEN LOWER(mapped_payment_method) = 'card' THEN COALESCE(total_amount_of_payments_paid, 0.0) ELSE 0.0 END AS total_spent_by_card,
            CASE WHEN LOWER(mapped_payment_method) = 'eft' THEN COALESCE(total_amount_of_payments_paid, 0.0) ELSE 0.0 END AS total_spent_by_eft,
            CASE WHEN LOWER(mapped_payment_method) = 'check' THEN COALESCE(total_amount_of_payments_paid, 0.0) ELSE 0.0 END AS total_spent_by_cheque,
            CASE WHEN LOWER(mapped_payment_method) = 'ach' THEN COALESCE(total_amount_of_payments_paid, 0.0) ELSE 0.0 END AS total_spent_by_ach,
            CASE WHEN LOWER(mapped_payment_method) NOT IN ('card','check','ach', 'eft') OR payment_method IS NULL
                 THEN COALESCE(total_amount_of_payments_paid, 0.0) ELSE 0.0 END AS total_spent_by_other,
            CASE WHEN LOWER(mapped_payment_method) = 'card' THEN 1 ELSE 0 END AS c_card_accepting_suppliers
        FROM ap_data
    ),
    ap_totals AS (
        SELECT
            ap_row_contrib.*,
            SUM(c_total_number_of_payment)       OVER () AS all_suppliers_total_number_of_payment,
            SUM(c_total_number_of_invoices)      OVER () AS all_suppliers_total_number_of_invoices,
            SUM(total_spend)                   OVER () AS all_suppliers_total_spend,
            SUM(total_spent_by_card)                 OVER () AS all_supplier_total_spent_by_card,
            SUM(total_spent_by_eft)                  OVER () AS all_supplier_total_spent_by_eft,
            SUM(total_spent_by_cheque)               OVER () AS all_supplier_total_spent_by_cheque,
            SUM(total_spent_by_ach)                  OVER () AS all_supplier_total_spent_by_ach,
            SUM(total_spent_by_other)                OVER () AS all_supplier_total_spent_by_other,
            SUM(c_card_accepting_suppliers)      OVER () AS all_suppliers_total_card_accepting_suppliers,
            COUNT(*)                             OVER () AS all_suppliers_total_number_of_suppliers
        FROM ap_row_contrib
    ),
    with_due_dates AS (
        SELECT *,
           CASE
               WHEN payment_term_days IS NULL THEN NULL
               ELSE DATE_ADD(CAST(ingestion_ts AS DATE), CAST(payment_term_days AS INT))
           END AS next_payment_date
        FROM ap_totals
    )
    SELECT * FROM with_due_dates
    """


def validate_result(result_df) -> None:
    """Fail fast if anything unexpected shows up"""
    missing = set(required_cols) - set(result_df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    unexpected = set(result_df.columns) - set(required_cols)
    if unexpected:
        raise ValueError(f"Unexpected columns found: {unexpected}")


# Upsert (delete+append) handle absence of table gracefully
def upsert_gold_table(spark: SparkSession, gold_tbl: str, result_df: DataFrame, file_id: str) -> None:
    try:
        spark.sql(f"DELETE FROM {gold_tbl} WHERE file_id = '{file_id}'")
    except AnalysisException:
        if not spark.catalog.tableExists(gold_tbl):
            print(f"Gold table {gold_tbl} not found will create on write...")
        else:
            raise
    result_df.select(required_cols).write.mode("append").saveAsTable(gold_tbl)


def matching(spark: SparkSession, input_df: DataFrame, args: Namespace) -> DataFrame:
    if args.match_type.lower() == "reltio":
        from reltio_matcher import perform_matching
        return perform_matching(spark, input_df, args)
    elif args.match_type.lower() == "in-house":
        from in_house_matcher import match_with_audit
        return match_with_audit(spark, args.model_path, args.src_clustered_tbl, input_df, args.in_house_audit_tbl)
    else:
        raise ValueError(f"Unknown match type: {args.match_type}")


def run_pipeline(spark, args, st) -> None:
    # Skip if already processed
    file_id_exists_and_not_failed = spark.table(args.ap_tracker_tbl).where(F.col("file_id") == args.file_id).where(F.col("status") != "FAILED")
    if file_id_exists_and_not_failed.head(1):
        _set_task_value("skip_reason", "file is already processed...")
        print(f"file_id = {args.file_id} already processed and its status is not FAILED. Skipping...")
        return

    update_audit_tbl(spark, args, 'IN-PROGRESS', 0, start_time=st, msg="")
    print(f"Inserted tracker record for file_id = {args.file_id} with status = IN-PROGRESS")

    # Build AP file dataframe
    file_id_df = (spark.sql(build_ap_query(args)).drop("ingestion_ts")
                  .withColumn("payment_terms", F.array(F.col("payment_terms")))
                  .withColumn("payment_method", F.array(F.col("mapped_payment_method"))))
    records = file_id_df.count()
    print(f"file_id={args.file_id} records={records}")

    # run matching...
    matched_result_df = matching(spark, file_id_df, args)
    matched_df = file_id_df.join(matched_result_df, on="supplier_id", how="left")

    # Merchant summary join...
    mer_summary_df = spark.read.table(args.mer_summary_tbl)
    result_df = matched_df.join(mer_summary_df, on="entity_id", how="left")

    confidence_cond = when(col("matched_score") >= 0.75, "High").when(
        (col("matched_score") < 0.75) & (col("matched_score") >= 0.7), "Medium").otherwise("Low")
    filtered_df = result_df.withColumn("confidence", confidence_cond).select(*required_cols)

    # Validate final result...
    validate_result(filtered_df)
    print(f"Before inserting to GOLD table {args.so_ap_bd_tbl}")
    upsert_gold_table(spark, args.so_ap_bd_tbl, filtered_df, args.file_id)
    update_audit_tbl(spark, args, "SUCCESS", records, start_time=st, msg="")
    print(f"Completed AP process for file_id={args.file_id} in {round((datetime.now() - st).total_seconds(), 2)}s")


def update_audit_tbl(spark, args, status, records, start_time, msg) -> None:
    print(f"Updating the audit table {args.ap_tracker_tbl} with status {status} for file_id {args.file_id} ")

    try:
        from datetime import datetime
        now = datetime.now()
        duration = round((now - start_time).total_seconds(), 2)

        # ensure safe strings
        safe_file_id = str(args.file_id or "").strip()
        safe_status = str(status or "").replace("'", "''")
        safe_msg = str(msg or "")[:5000].replace("'", "''")
        safe_start = str(now)

        spark.sql(f"""
                MERGE INTO {args.ap_tracker_tbl} AS tgt
                USING (
                SELECT
                    '{safe_file_id}'                       AS file_id_key,
                    TIMESTAMP('{safe_start}')              AS start_dt,
                    current_timestamp()                    AS last_processed_dt,
                    {float(duration)}                      AS total_time_taken,
                    {int(records)}                         AS record_count,
                    '{safe_status}'                        AS status,
                    '{safe_msg}'                           AS comment
                ) AS src
                ON trim(tgt.file_id) = trim(src.file_id_key)
                WHEN MATCHED THEN UPDATE SET
                tgt.last_processed_dt = src.last_processed_dt,
                tgt.total_time_taken  = src.total_time_taken,
                tgt.record_count      = src.record_count,
                tgt.status            = src.status,
                tgt.comment           = src.comment
                WHEN NOT MATCHED THEN INSERT (
                file_id, start_dt, last_processed_dt, total_time_taken, record_count, status, comment
                ) VALUES (
                src.file_id_key, src.start_dt, src.last_processed_dt, src.total_time_taken, src.record_count, src.status, src.comment
                )
        """)
    except Exception as e:
        print(f"failed to merge, inserting instead... {e}")
        spark.sql(f"""INSERT INTO {args.ap_tracker_tbl} (file_id, start_dt, last_processed_dt, total_time_taken, record_count, status, comment)
                VALUES (
                    '{args.file_id}',
                    TIMESTAMP('{start_time}'),
                    current_timestamp(),
                    {float(duration)},
                    0,
                    '{status}',
                    '{msg}'
                )
            """)


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AP File Processing pipeline (with match caching)")
    p.add_argument("--file-id", required=True)
    p.add_argument("--ptm-tbl", default="mc_sme.bd.payment_terms_mapping")
    p.add_argument("--pm-tbl", default="mc_sme.bd.payment_mapping")
    p.add_argument("--ap-tracker-tbl", required=True)
    p.add_argument("--ap-raw-tbl", required=True)
    p.add_argument("--reltio-match-audit-tbl", required=True)
    p.add_argument("--mer-summary-tbl", required=True)
    p.add_argument("--so-ap-bd-tbl", required=True)
    p.add_argument("--reltio-match-url", required=True, help="https://.../entities/_matches")
    p.add_argument("--reltio-auth-url", required=True, help="https://.../entities/_matches")
    p.add_argument("--match-batch-size", type=int, default=200)
    p.add_argument("--match-max-concurrent-batches", type=int, default=5)
    p.add_argument("--match-max-retries", type=int, default=3)
    p.add_argument("--match-backoff-secs", type=float, default=1.0)
    p.add_argument("--match-connect-timeout", type=int, default=5)
    p.add_argument("--match-read-timeout", type=int, default=60)
    p.add_argument("--match-cache-expiry", type=int, default=7)
    p.add_argument("--match-type", default="reltio", help="either reltio or in-house")
    p.add_argument("--model-path", default="", help="model path for in-house matcher")
    p.add_argument("--src-clustered-tbl", default="", help="src_clustered_tbl for in-house matcher")
    p.add_argument("--in-house-audit-tbl", default="", help="audit table for in-house matcher")
    return p.parse_args(argv)


def main() -> None:  # pragma no cover
    st = datetime.now()
    args = parse_args(sys.argv[1:])
    print(f"Job started with args: {args}")
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("AP_File_Pipeline").getOrCreate()
    try:
        run_pipeline(spark, args, st)
    except Exception as e:
        print(f"Error occurred during pipeline execution: {e}")
        traceback.print_exc()
        # Failure: call audit update with FAILED
        update_audit_tbl(spark, args, "FAILED", records=0, start_time=st, msg=str(e))
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":  # pragma no cover
    main()
