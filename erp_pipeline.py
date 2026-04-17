from __future__ import annotations

import argparse
import sys
import traceback
import logging
from argparse import Namespace
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, when
from pyspark.sql.types import DoubleType, DecimalType
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# To ensure all required columns are present in the final output table
required_cols = ['entity_id', 'supplier_name', 'merchant_id', 'date_period', 'supplier_id', 'street_address', 'city', 'state', 'postal_code',
                 'country', 'phone_number', 'email_address', 'currency', 'payment_method', 'payment_terms', 'total_number_of_invoices',
                 'total_amount_of_invoices', 'total_number_of_purchase_orders', 'total_amount_of_purchase_orders', 'total_number_of_payments_paid',
                 'total_amount_of_payments_paid', 'total_number_of_payments_due', 'total_amount_of_payments_due', 'total_number_of_payments_open',
                 'total_amount_of_payments_open', 'total_amount_spend', 'total_spent_by_cheque', 'total_spent_by_card', 'total_spent_by_ach',
                 'total_spent_by_eft', 'total_spent_by_other', 'next_payment_date', 'annual_target_spend', 'transaction_count', 'average_ticket',
                 'is_matched', 'supplier_website_url', 'region', 'mcc_code', 'supplier_alias_name', 'industry_classification_code',
                 'preferred_payment_method', 'propensity_score', 'transaction_recency', 'sic', 'suggested_talking_points', 'cleansed_supplier_name',
                 'cleansed_address_line_1', 'cleansed_city_name', 'Cleansed_state_Or_province_name', 'cleansed_country_code', 'cleansed_postal_code',
                 'aggregate_merchant_name', 'aggregate_merchant_id', 'parent_aggregate_merchant_name', 'parent_aggregate_merchant_id', 'mcc_group',
                 'business_region_name', 'commercial_history', 'commercial_recency', 'clearing_last_seen_date', 'auth_last_seen_date', 'avg_tran_amt',
                 'matched_trans_count', 'industry', 'in_control_history', 'in_control_recency', 'last_update_date', 'customer_naics', 'mmh_id',
                 'card_acceptor', 'confidence', 'propensity_score_label', 'all_suppliers_total_number_of_payments',
                 'all_suppliers_total_number_of_suppliers', 'all_suppliers_total_number_of_invoices', 'all_suppliers_card_accepting_suppliers']

JOB_NAME = "erp_workflow"


def _get_dbutils():
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        return DBUtils(SparkSession.getActiveSession())
    except Exception:
        return None


def _set_task_value(key: str, value: str) -> None:
    dbutils = _get_dbutils()
    try:
        if dbutils:
            dbutils.jobs.taskValues.set(key=key, value=value)  # type: ignore
    except Exception:
        pass


def validate_result(result_df) -> None:
    """Fail fast if anything unexpected shows up"""
    missing = set(required_cols) - set(result_df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    unexpected = set(result_df.columns) - set(required_cols)
    if unexpected:
        raise ValueError(f"Unexpected columns found: {unexpected}")


def get_supplier_info(spark: SparkSession, args: Namespace, merchant_ids_str: str):
    """Fetches latest updated supplier details """
    query = f"""WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY merchant_id, supplier_id ORDER BY ingestion_ts DESC) AS rn
            FROM {args.erp_suppliers_tbl}
            WHERE merchant_id IN ({merchant_ids_str})
              AND soft_delete = false
        ),
        latest_suppliers AS (
            SELECT * FROM ranked WHERE rn = 1
        )
        SELECT 'monthly' AS date_period, s.*
        FROM latest_suppliers s
        UNION ALL
        SELECT 'quarterly' AS date_period, s.*
        FROM latest_suppliers s
        UNION ALL
        SELECT 'yearly' AS date_period, s.*
        FROM latest_suppliers s"""
    supplier_df = spark.sql(query)
    return supplier_df


def get_po_info(spark: SparkSession, args: Namespace, merchant_ids_str: str):
    """Fetches Purchase orders raw data of last 365 days """
    query = f"""SELECT * FROM {args.erp_po_tbl} WHERE merchant_id IN ({merchant_ids_str})
                AND modified_date >= CURRENT_DATE - INTERVAL 365 DAYS AND soft_delete = false"""
    po_df = spark.sql(query)
    return po_df


def get_invoice_info(spark: SparkSession, args: Namespace, merchant_ids_str: str):
    """Fetches invoice  raw data of last 365 days """
    query = f"""SELECT * FROM {args.erp_invoice_tbl} WHERE merchant_id IN ({merchant_ids_str})
            AND modified_date >= CURRENT_DATE - INTERVAL 365 DAYS AND soft_delete = false"""
    invoice_df = spark.sql(query)
    return invoice_df


def get_payment_info(spark: SparkSession, args: Namespace, merchant_ids_str: str):
    """Fetches last 365 days of payment data from raw table"""
    query = f"""SELECT p.*, pm.sme_payment_method AS mapped_payment_method
        FROM {args.erp_payments_tbl} p
        LEFT JOIN {args.payment_mapping_tbl} pm
          ON LOWER(TRIM(p.payment_method)) = LOWER(pm.src_payment_method)
        WHERE merchant_id IN ({merchant_ids_str}) AND modified_date >= CURRENT_DATE - INTERVAL 365 DAYS AND soft_delete = false"""
    payment_df = spark.sql(query)
    return payment_df


def agg_invoice(spark: SparkSession, invoice_df: DataFrame):
    """Aggregates invoice data to get monthly, quarterly and yearly aggregations"""
    invoice_df.createOrReplaceTempView("invoice_data")
    agg_query = """SELECT 'monthly' AS date_period, merchant_id, supplier_id,
               collect_set(payment_terms) AS payment_terms,
               COUNT(*) AS total_number_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ("PAID", "SUBMITTED", "PARTIALLY_PAID", "OPEN") THEN 1 END) AS total_number_of_payments_paid_tbg,
               SUM(COALESCE(amount, 0)) AS total_amount_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status))='PAID' THEN 1 END) AS total_number_of_payments_paid,
               SUM(CASE WHEN UPPER(TRIM(status))='PAID' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_paid,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN 1 END) AS total_number_of_payments_due,
               SUM(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_due,
               COUNT(CASE WHEN UPPER(TRIM(status))='OPEN' THEN 1 END) AS total_number_of_payments_open,
               SUM(CASE WHEN UPPER(TRIM(status))='OPEN' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_open,
               MIN(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'OPEN') AND due_date >= CURRENT_DATE THEN due_date END) AS next_payment_date
        FROM invoice_data
        WHERE modified_date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'quarterly' AS date_period, merchant_id, supplier_id,
               collect_set(payment_terms) AS payment_terms,
               COUNT(*) AS total_number_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ("PAID", "SUBMITTED", "PARTIALLY_PAID", "OPEN") THEN 1 END) AS total_number_of_payments_paid_tbg,
               SUM(COALESCE(amount, 0)) AS total_amount_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status))='PAID' THEN 1 END) AS total_number_of_payments_paid,
               SUM(CASE WHEN UPPER(TRIM(status))='PAID' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_paid,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN 1 END) AS total_number_of_payments_due,
               SUM(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_due,
               COUNT(CASE WHEN UPPER(TRIM(status))='OPEN' THEN 1 END) AS total_number_of_payments_open,
               SUM(CASE WHEN UPPER(TRIM(status))='OPEN' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_open,
               MIN(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'OPEN') AND due_date >= CURRENT_DATE THEN due_date END) AS next_payment_date
        FROM invoice_data
        WHERE modified_date >= CURRENT_DATE - INTERVAL 90 DAYS
        GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'yearly' AS date_period, merchant_id, supplier_id,
               collect_set(payment_terms) AS payment_terms,
               COUNT(*) AS total_number_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ("PAID", "SUBMITTED", "PARTIALLY_PAID", "OPEN") THEN 1 END) AS total_number_of_payments_paid_tbg,
               SUM(COALESCE(amount, 0)) AS total_amount_of_invoices,
               COUNT(CASE WHEN UPPER(TRIM(status))='PAID' THEN 1 END) AS total_number_of_payments_paid,
               SUM(CASE WHEN UPPER(TRIM(status))='PAID' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_paid,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN 1 END) AS total_number_of_payments_due,
               SUM(CASE WHEN UPPER(TRIM(status)) IN ('OPEN', 'SUBMITTED') AND due_date < CURRENT_DATE THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_due,
               COUNT(CASE WHEN UPPER(TRIM(status))='OPEN' THEN 1 END) AS total_number_of_payments_open,
               SUM(CASE WHEN UPPER(TRIM(status))='OPEN' THEN COALESCE(amount, 0) ELSE 0 END) AS total_amount_of_payments_open,
               MIN(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'OPEN') AND due_date >= CURRENT_DATE THEN due_date END) AS next_payment_date
        FROM invoice_data GROUP BY merchant_id, supplier_id"""
    agg_invoice_df = spark.sql(agg_query)
    return agg_invoice_df


def get_po_agg(spark: SparkSession, po_df: DataFrame):
    """Aggregates purchase order data to get monthly, quarterly and yearly aggregations"""
    po_df.createOrReplaceTempView("purchase_order_data")
    po_agg_query = """SELECT 'monthly' AS date_period, merchant_id, supplier_id,
               COUNT(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN 1 END) AS total_number_of_purchase_orders,
               SUM(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN COALESCE(amount, 0) END) AS total_amount_of_purchase_orders
        FROM purchase_order_data WHERE modified_date >= CURRENT_DATE - INTERVAL 30 DAYS GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'quarterly' AS date_period, merchant_id, supplier_id,
            COUNT(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN 1 END) AS total_number_of_purchase_orders,
            SUM(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN COALESCE(amount, 0) END) AS total_amount_of_purchase_orders
        FROM purchase_order_data WHERE modified_date >= CURRENT_DATE - INTERVAL 90 DAYS GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'yearly' AS date_period, merchant_id, supplier_id,
            COUNT(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN 1 END) AS total_number_of_purchase_orders,
            SUM(CASE WHEN UPPER(TRIM(status)) IN ('SUBMITTED', 'AUTHORIZED', 'BILLED') THEN COALESCE(amount, 0) END) AS total_amount_of_purchase_orders
        FROM purchase_order_data GROUP BY merchant_id, supplier_id"""
    agg_po_df = spark.sql(po_agg_query)
    return agg_po_df


def payment_agg(spark: SparkSession, payment_df: DataFrame):
    """Aggregates purchase order data to get monthly, quarterly and yearly aggregations"""
    payment_df.createOrReplaceTempView("payment_data")
    payment_agg_query = """SELECT 'monthly' AS date_period, merchant_id, supplier_id,
               collect_set(mapped_payment_method) AS payment_method,
               COUNT(*) AS totalNumberOfPayments,
               COUNT(DISTINCT CASE WHEN UPPER(mapped_payment_method) = 'CARD' THEN supplier_id END) AS total_number_of_card_accepting_suppliers,
               SUM(COALESCE(amount, 0)) AS total_amount_spend,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CARD' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_card,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CHECK' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_cheque,
               SUM(CASE WHEN UPPER(mapped_payment_method)='ACH' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_ach,
               SUM(CASE WHEN UPPER(mapped_payment_method) NOT IN ('CARD', 'CHECK', 'ACH') OR mapped_payment_method is NULL THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_other
        FROM payment_data
        WHERE modified_date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'quarterly' AS date_period, merchant_id, supplier_id,
               collect_set(mapped_payment_method) AS payment_method,
               COUNT(*) AS totalNumberOfPayments,
               COUNT(DISTINCT CASE WHEN UPPER(mapped_payment_method) = 'CARD' THEN supplier_id END) AS total_number_of_card_accepting_suppliers,
               SUM(COALESCE(amount, 0)) AS total_amount_spend,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CARD' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_card,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CHECK' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_cheque,
               SUM(CASE WHEN UPPER(mapped_payment_method)='ACH' THEN COALESCE(amount, 0) ELSE 0 END)  AS total_spent_by_ach,
               SUM(CASE WHEN UPPER(mapped_payment_method) NOT IN ('CARD', 'CHECK', 'ACH') OR mapped_payment_method is NULL THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_other
        FROM payment_data
        WHERE modified_date >= CURRENT_DATE - INTERVAL 90 DAYS
        GROUP BY merchant_id, supplier_id
        UNION ALL
        SELECT 'yearly' AS date_period, merchant_id, supplier_id,
               collect_set(mapped_payment_method) AS payment_method,
               COUNT(*) AS totalNumberOfPayments,
               COUNT(DISTINCT CASE WHEN UPPER(mapped_payment_method) = 'CARD' THEN supplier_id END) AS total_number_of_card_accepting_suppliers,
               SUM(COALESCE(amount, 0)) AS total_amount_spend,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CARD' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_card,
               SUM(CASE WHEN UPPER(mapped_payment_method)='CHECK' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_cheque,
               SUM(CASE WHEN UPPER(mapped_payment_method)='ACH' THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_ach,
               SUM(CASE WHEN UPPER(mapped_payment_method) NOT IN ('CARD', 'CHECK', 'ACH') OR mapped_payment_method is NULL THEN COALESCE(amount, 0) ELSE 0 END) AS total_spent_by_other
        FROM payment_data
        GROUP BY merchant_id, supplier_id"""
    payment_agg_df = spark.sql(payment_agg_query)
    return payment_agg_df


def join_final_metrics(spark: SparkSession, supplier_df: DataFrame, invoice_agg_df: DataFrame, po_agg_df: DataFrame, payment_agg_df: DataFrame) -> DataFrame:
    """Joins supplier_info, payment aggregation, invoice aggregation and purchase order aggregation to get final aggregation"""
    supplier_df.createOrReplaceTempView("supplier_info")
    invoice_agg_df.createOrReplaceTempView("invoice_agg")
    po_agg_df.createOrReplaceTempView("po_agg")
    payment_agg_df.createOrReplaceTempView("payment_agg")
    final_agg_query = """SELECT
            s.merchant_id,
            s.date_period,
            s.supplier_id,
            s.supplier_name,
            s.street_address,
            s.city,
            s.state,
            s.postal_code,
            s.country,
            s.phone_number,
            s.email_address,
            s.currency,
            s.issuer_id,
            p.payment_method,
            p.total_number_of_card_accepting_suppliers,
            i.payment_terms,
            COALESCE(i.total_number_of_invoices,0) AS total_number_of_invoices,
            COALESCE(i.total_number_of_payments_paid_tbg,0) AS total_number_of_payments_paid_tbg,
            COALESCE(i.total_amount_of_invoices,0) AS total_amount_of_invoices,
            COALESCE(po.total_number_of_purchase_orders,0) AS total_number_of_purchase_orders,
            COALESCE(po.total_amount_of_purchase_orders,0) AS total_amount_of_purchase_orders,
            COALESCE(i.total_number_of_payments_paid,0) AS total_number_of_payments_paid,
            COALESCE(i.total_amount_of_payments_paid,0) AS total_amount_of_payments_paid,
            COALESCE(i.total_number_of_payments_due,0) AS total_number_of_payments_due,
            COALESCE(i.total_amount_of_payments_due,0) AS total_amount_of_payments_due,
            COALESCE(i.total_number_of_payments_open,0) AS total_number_of_payments_open,
            COALESCE(i.total_amount_of_payments_open,0) AS total_amount_of_payments_open,
            COALESCE(p.total_amount_spend,0) AS total_amount_spend,
            COALESCE(p.total_spent_by_cheque,0) AS total_spent_by_cheque,
            COALESCE(p.total_spent_by_card,0) AS total_spent_by_card,
            COALESCE(p.total_spent_by_ach,0) AS total_spent_by_ach,
            CAST(0.0 AS DECIMAL(38, 2)) AS total_spent_by_eft,
            COALESCE(p.total_spent_by_other,0) AS total_spent_by_other,
            i.next_payment_date
        FROM supplier_info s
        LEFT JOIN po_agg po
          ON s.merchant_id = po.merchant_id AND s.supplier_id = po.supplier_id AND s.date_period = po.date_period
        LEFT JOIN invoice_agg i
          ON s.merchant_id = i.merchant_id AND s.supplier_id = i.supplier_id AND s.date_period = i.date_period
        LEFT JOIN payment_agg p
          ON s.merchant_id = p.merchant_id AND s.supplier_id = p.supplier_id AND s.date_period = p.date_period"""
    final_agg_data = spark.sql(final_agg_query)
    return final_agg_data


def agg_erp_metrics(spark: SparkSession, merchant_ids, args):
    """Gets all aggregation for final aggregations metrics."""
    logging.info(f"Aggregating ERP metrics for merchant_ids {merchant_ids}")
    merchant_ids_str = ",".join(f"'{item}'" for item in merchant_ids)
    supplier_df = get_supplier_info(spark, args, merchant_ids_str)
    invoice_df = get_invoice_info(spark, args, merchant_ids_str)
    po_df = get_po_info(spark, args, merchant_ids_str)
    payment_df = get_payment_info(spark, args, merchant_ids_str)
    invoice_agg_df = agg_invoice(spark, invoice_df)
    po_agg_df = get_po_agg(spark, po_df)
    payment_agg_df = payment_agg(spark, payment_df)
    final_df = join_final_metrics(spark, supplier_df, invoice_agg_df, po_agg_df, payment_agg_df)
    return final_df


# Aggregate Merchant level aggregates for total number of invoices, supplier,payments and total_card_accepting_suppliers
def get_erp_merchant_agg(spark: SparkSession, df: DataFrame):
    df.createOrReplaceTempView("erp_merchant_supplier_agg")
    merchant_agg_sql_query = """
    WITH
    -- Total Payments per Merchant
    payments_per_merchant AS (
        SELECT merchant_id, date_period, SUM(COALESCE(total_number_of_payments_paid,0) + COALESCE(total_number_of_payments_open,0) ) AS all_suppliers_total_number_of_payments FROM erp_merchant_supplier_agg GROUP BY merchant_id, date_period
    ),
    -- Total Suppliers per Merchant
    suppliers_per_merchant AS (
        SELECT merchant_id, date_period, COUNT(DISTINCT supplier_id) AS all_suppliers_total_number_of_suppliers FROM erp_merchant_supplier_agg GROUP BY merchant_id, date_period
    ),
    -- Total Invoices per Merchant
    invoices_per_merchant AS (
        SELECT merchant_id, date_period, SUM(total_number_of_payments_paid_tbg) AS all_suppliers_total_number_of_invoices FROM erp_merchant_supplier_agg GROUP BY merchant_id, date_period
    ),
    -- Total Number of suppliers accepting cards
    card_accepting_suppliers AS (
        SELECT merchant_id, date_period, COALESCE(COUNT(DISTINCT CASE WHEN total_number_of_card_accepting_suppliers != 0 THEN supplier_id END), 0) AS all_suppliers_card_accepting_suppliers FROM erp_merchant_supplier_agg GROUP BY merchant_id, date_period
    )
    SELECT
        s.*,
        ppm.all_suppliers_total_number_of_payments,
        spm.all_suppliers_total_number_of_suppliers,
        ipm.all_suppliers_total_number_of_invoices,
        cas.all_suppliers_card_accepting_suppliers
    FROM erp_merchant_supplier_agg s
    LEFT JOIN payments_per_merchant ppm
        ON s.merchant_id = ppm.merchant_id and s.date_period = ppm.date_period
    LEFT JOIN suppliers_per_merchant spm
        ON s.merchant_id = spm.merchant_id and s.date_period = spm.date_period
    LEFT JOIN invoices_per_merchant ipm
        ON s.merchant_id = ipm.merchant_id and s.date_period = ipm.date_period
    LEFT JOIN card_accepting_suppliers cas
        ON s.merchant_id = cas.merchant_id and s.date_period = cas.date_period
    """
    return spark.sql(merchant_agg_sql_query)


# Upsert (delete+append); handle absence of table gracefully
def upsert_gold_table(spark: SparkSession, gold_tbl: str, df: DataFrame, merchant_ids: List[str]):
    try:
        if merchant_ids:
            merchant_ids_str = ",".join(f"'{item}'" for item in merchant_ids)
            spark.sql(f"DELETE FROM {gold_tbl} WHERE merchant_id IN ({merchant_ids_str})")
    except AnalysisException:
        if not spark.catalog.tableExists(gold_tbl):
            logging.info(f"Gold table {gold_tbl} not found; will create on write...")
        else:
            raise
    df.write.mode("append").saveAsTable(gold_tbl)


def write_process_audit(spark: SparkSession, process_audit_tbl: str, merchant_ids: List[str], start_dt: datetime, end_dt: datetime):
    if not merchant_ids:
        return
    duration = round((end_dt - start_dt).total_seconds(), 2)
    df = spark.createDataFrame([(m,) for m in merchant_ids], ["merchant_id"])
    df = (df
          .withColumn("start_dt", lit(start_dt))
          .withColumn("end_dt", lit(end_dt))
          .withColumn("time_taken", lit(duration).cast(DoubleType()))
          .withColumn("status", lit("SUCCESS")))
    df.write.mode("append").saveAsTable(process_audit_tbl)


def matching(spark: SparkSession, input_df: DataFrame, args: Namespace) -> DataFrame:
    if args.match_type.lower() == "reltio":
        # from reltio_matcher import perform_matching
        from reltio_matching_service import perform_matching
        return perform_matching(spark, input_df, args)
    elif args.match_type.lower() == "in-house":
        from in_house_matcher import match_with_audit
        return match_with_audit(spark, args.model_path, args.src_clustered_tbl, input_df, args.in_house_audit_tbl)
    else:
        raise ValueError(f"Unknown match type: {args.match_type}")


def update_erp_run_audit(spark, tracker_tbl: str, run_id: str, merchant_ids: list, start_time, status: str, msg: str = None) -> None:
    logging.info(f"[AUDIT] Updating {tracker_tbl} | run_id={run_id} | status={status} | Merchant ids ={merchant_ids} | msg={msg}")
    merchants_sql = ",".join([f"{m}" for m in merchant_ids])
    try:
        now = datetime.now()
        duration = round((now - start_time).total_seconds(), 2)

        # Safe values
        safe_run_id = run_id.replace("'", "''")
        safe_status = status.replace("'", "''")
        safe_msg = (msg or "")[:5000].replace("'", "''")
        spark.sql(f"""
                MERGE INTO {tracker_tbl} AS tgt
                USING (
                    SELECT
                        '{safe_run_id}'                  AS run_id,
                        '{merchants_sql}'                AS merchant_ids,
                        {len(merchant_ids)}              AS number_of_merchants,
                        TIMESTAMP('{start_time.strftime('%Y-%m-%d %H:%M:%S')}') AS start_time,
                        TIMESTAMP('{now.strftime('%Y-%m-%d %H:%M:%S')}')        AS end_time,
                        {duration}                       AS time_taken_seconds,
                        '{safe_status}'                  AS status,
                        '{safe_msg}'                     AS error_message,
                        current_timestamp()              AS updated_at
                ) src
                ON tgt.run_id = src.run_id
                WHEN MATCHED THEN UPDATE SET
                    tgt.end_time             = src.end_time,
                    tgt.time_taken_seconds   = src.time_taken_seconds,
                    tgt.status               = src.status,
                    tgt.error_message        = src.error_message,
                    tgt.updated_at           = src.updated_at
                WHEN NOT MATCHED THEN INSERT (run_id, merchant_ids, number_of_merchants, start_time, end_time, time_taken_seconds, status, error_message, updated_at)
                VALUES (src.run_id, src.merchant_ids, src.number_of_merchants, src.start_time, src.end_time, src.time_taken_seconds, src.status, src.error_message, current_timestamp())
            """)
    except Exception as e:
        # Last-resort fallback
        logging.info(f"[AUDIT][ERROR] Merge failed, inserting fallback row: {e}")
        spark.sql(f"""
            INSERT INTO {tracker_tbl} (run_id, merchant_ids, start_time, status, error_message, updated_at)
            VALUES ('{run_id}', '{merchants_sql}', TIMESTAMP('{start_time.strftime('%Y-%m-%d %H:%M:%S')}'), '{status}', '{(msg or str(e)).replace("'", "''")[:5000]}', current_timestamp()) """)


def run_pipeline(spark, args: Namespace, merchant_ids, start_time, run_id) -> int:
    logging.info(f"Updating audit tracker table {args.erp_tracker_tbl}")
    update_erp_run_audit(spark=spark, tracker_tbl=args.erp_tracker_tbl, run_id=run_id, merchant_ids=merchant_ids, start_time=start_time, status="RUNNING")
    logging.info("Performing Raw Data aggregations.")
    agg_df = agg_erp_metrics(spark, merchant_ids, args)

    unique_agg_supp_df = agg_df.dropDuplicates(["supplier_id", "supplier_name", "state", "city", "postal_code", "country", "street_address"])
    logging.info("Performing Matching...")
    matched_result_df = matching(spark, unique_agg_supp_df, args)
    matched_df = agg_df.join(matched_result_df, "supplier_id", "left")
    mch_df = spark.read.table(args.member_catgry_hier_tbl)
    matched_df = (matched_df.join(mch_df, matched_df["matched_mmc_code"] == mch_df["mcc_code"], how="left")
                  .withColumn("mcc_code", F.coalesce(F.col("mcc_code"), F.col("matched_mmc_code"))))

    confidence_col = when(col('matched_score') >= 0.75, "High").when((col('matched_score') < 0.75) & (col('matched_score') >= 0.70), "Medium").otherwise("Low")
    logging.info(f"Joining with merchant aggregate summary table.{args.mer_summary_tbl}")
    mer_summ_cols = ["entity_id", "preferred_payment_method", "propensity_score", "transaction_recency", "suggested_talking_points",
                     "commercial_history", "commercial_recency", "avg_tran_amt", "matched_trans_count", "in_control_history", "in_control_recency",
                     "propensity_score_label", "mmh_id", "card_acceptor"]
    mer_summary_df = spark.read.table(args.mer_summary_tbl).select(*mer_summ_cols)
    mer_summary_df = mer_summary_df.withColumnRenamed("entity_id", "mer_summary_entity_id")
    # mer_agg_summ_df = matched_df.join(mer_summary_df, on="entity_id", how="left")
    mer_agg_summ_df = (matched_df.join(mer_summary_df, matched_df.matched_mmhid == mer_summary_df.mmh_id, how="left")
                       .withColumn("mmh_id", F.coalesce(F.col("mmh_id"), F.col("matched_mmhid"))))
    mer_agg_summ_df = (mer_agg_summ_df.withColumnRenamed("matched_url", "supplier_website_url")
                       .withColumnRenamed("matched_sic", "sic").withColumnRenamed("matched_street_address", "cleansed_address_line_1")
                       .withColumnRenamed("matched_city", "cleansed_city_name").withColumnRenamed("matched_state", "Cleansed_state_Or_province_name")
                       .withColumnRenamed("matched_country_cd", "cleansed_country_code").withColumnRenamed("matched_postal_code", "cleansed_postal_code")
                       .withColumnRenamed("matched_agg_merch_name", "aggregate_merchant_name")
                       .withColumnRenamed("matched_agg_merch_id", "aggregate_merchant_id")
                       .withColumnRenamed("matched_parent_agg_merch_name", "parent_aggregate_merchant_name")
                       .withColumnRenamed("matched_parent_agg_merch_id", "parent_aggregate_merchant_id")
                       .withColumnRenamed("matched_clearing_last_seen_date", "clearing_last_seen_date")
                       .withColumnRenamed("matched_auth_last_seen_date", "auth_last_seen_date")
                       .withColumnRenamed("matched_zi_c_last_updated_date", "last_update_date")
                       .withColumnRenamed("matched_naics", "customer_naics").withColumnRenamed("matched_region_code", "dw_merch_region_cd")
                       .withColumnRenamed("matched_industry", "industry")
                       .withColumn("supplier_alias_name", col("matched_dba_name")).withColumn("cleansed_supplier_name", col("matched_dba_name"))
                       .withColumn("business_region_name", col("matched_region_name")).withColumn("region", col("matched_region_name")))
    result_df = get_erp_merchant_agg(spark, mer_agg_summ_df)

    # Adding Confidence, annual_target_spend, transaction_count, average_ticket columns.
    result_df = (result_df
                 .withColumn('confidence', confidence_col)
                 .withColumn("annual_target_spend", lit(0.0).cast(DecimalType(38, 2)))
                 .withColumn("transaction_count", lit(0).cast("long"))
                 .withColumn("average_ticket", lit(0).cast(DecimalType(38, 2))).select(required_cols))

    validate_result(result_df)
    logging.info(f"Inserting to GOLD table {args.so_erp_bd_tbl}")
    upsert_gold_table(spark, args.so_erp_bd_tbl, result_df, merchant_ids)
    print("ERP pipeline completed successfully.")
    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:  # pragma no cover
    p = argparse.ArgumentParser(description="ERP Pipeline Job")
    p.add_argument("--merchant-ids", required=True)
    p.add_argument("--erp-payments-tbl", default="mc_sme.bd.erp_payments_raw", help="ERP payments raw table")
    p.add_argument("--erp-suppliers-tbl", default="mc_sme.bd.erp_suppliers_raw", help="ERP suppliers raw table")
    p.add_argument("--erp-invoice-tbl", default="mc_sme.bd.erp_invoices_raw", help="ERP invoices raw table")
    p.add_argument("--erp-po-tbl", default="mc_sme.bd.erp_purchase_orders_raw", help="ERP purchase order raw table")
    p.add_argument("--payment_mapping_tbl", default="mc_sme.bd.payment_mapping", help="Payment methods mapping table for AP/ERP")
    p.add_argument("--erp-tracker-tbl", required=True, help="ERP process audit tracker table")
    p.add_argument("--mer-summary-tbl", required=True, help="Merchant summary (mer_summary_tbl)")
    p.add_argument("--member-catgry-hier-tbl", required=True)
    p.add_argument("--reltio-match-audit-tbl", required=True, help="Match cache / audit table (for joining)")
    p.add_argument("--so-erp-bd-tbl", required=True, help="Final ERP merchant supplier aggregation table")
    p.add_argument("--max-merchant-ids", type=int, default=5000, help="Safety cap to avoid giant IN clauses")
    p.add_argument("--reltio-match-url", required=True, help="https://.../entities/_matches")
    p.add_argument("--reltio-search-url", required=True, help="https://.../entities/_matches")
    p.add_argument("--reltio-auth-url", required=True, help="https://.../entities/_matches")
    p.add_argument("--reltio-secret-name", required=True, help="aws secret name for reltio access token")
    p.add_argument("--match-batch-size", type=int, default=200)
    p.add_argument("--match-max-concurrent-batches", type=int, default=5)
    p.add_argument("--match-max-retries", type=int, default=3)
    p.add_argument("--match-backoff-secs", type=float, default=1.0)
    p.add_argument("--match-connect-timeout", type=int, default=120)
    p.add_argument("--match-read-timeout", type=int, default=60)
    p.add_argument("--match-cache-expiry", type=int, default=7)
    p.add_argument("--search-max-workers", type=int, default=50)
    p.add_argument("--min-match-score", type=float, default=0.80)
    p.add_argument("--match-type", default="reltio", help="either reltio or in-house")
    p.add_argument("--model-path", default="", help="model path for in-house matcher")
    p.add_argument("--src-clustered-tbl", default="", help="src_clustered_tbl for in-house matcher")
    p.add_argument("--in-house-audit-tbl", default="", help="audit table for in-house matcher")
    p.add_argument("--job-run-id", help="The Databricks Job Run ID")
    p.add_argument("--country-map-table", default="mc_sme.bd.country_map")
    p.add_argument("--country-alias-map-table", default="mc_sme.bd.country_alias_map")

    return p.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    logging.info(f"job started with args:{args}")
    start_time = datetime.now()
    merchant_ids = args.merchant_ids.split(",")
    merchant_ids = [m.strip() for m in merchant_ids]
    logging.info(f"Merchant ids passed = {merchant_ids}")
    run_id = args.job_run_id
    logging.info(f"The current Job Run ID is: {run_id}")
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ERP_Pipeline").getOrCreate()
    try:
        run_pipeline(spark, args, merchant_ids, start_time, run_id)
        logging.info("Updating audit table with SUCCESS")
        update_erp_run_audit(spark=spark, tracker_tbl=args.erp_tracker_tbl, run_id=run_id, merchant_ids=merchant_ids, start_time=start_time, status="SUCCEEDED")
        logging.info("ERP pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Error occurred during pipeline execution: {e}")
        traceback.print_exc()
        # Update failed status in run tracking table
        update_erp_run_audit(spark=spark, tracker_tbl=args.erp_tracker_tbl, run_id=run_id, merchant_ids=merchant_ids, start_time=start_time, status="FAILED", msg=traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":  # pragma no cover
    main()
