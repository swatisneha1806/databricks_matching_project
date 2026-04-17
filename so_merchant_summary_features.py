import argparse
import sys
from datetime import datetime, timedelta, date
from typing import List
from pyspark.sql import Window as W
import mlflow
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType


def get_merchant_transactions(spark, max_process_date: datetime.date, roll_back_days: int, args) -> DataFrame:
    items = args.merch_region_cd.replace(' ', '').split(",")
    df_cutc = spark.read.table(args.sme_clring_tbl) \
        .filter(col('dw_merch_region_cd').isin(*[f'{i}' for i in items + [str(int(i)) for i in items]])) \
        .filter(
        f.to_date(col("process_date")).between(
            f.date_sub(f.lit(max_process_date), roll_back_days - 1), f.lit(max_process_date)
        )
    ).filter(col('dw_net_pd_amt') >= 0)

    cutc_loc_join = ((col('cutc.dw_merch_location_id') == col('mmh_loc.old_loc_id')) & (
        col('cutc.dw_merch_region_cd').cast('int') == col('mmh_loc.cleansed_business_region_code').cast('int')))

    mmh_rlto_join = ((col('mmh_loc.merchant_market_hierarchy_id') == col('rlto.mmh_id')) & (
        col('mmh_loc.cleansed_business_region_code').cast('int') == col('rlto.region_code').cast('int')))

    df_mmh_loc = spark.read.table(args.mmh_loc_tbl) \
        .filter(col('cleansed_business_region_code').isin(*[f'{i}' for i in items + [str(int(i)) for i in items]]))

    df_rlto = (spark.read.table(args.emd_flatt_tbl).select("entity_id", "mcc_code", explode('mmh_id').alias("mmh_id"), "region_code"))

    return (df_cutc.alias('cutc').join(df_mmh_loc.alias('mmh_loc'), cutc_loc_join, "inner").join(df_rlto.alias('rlto'), mmh_rlto_join, "left"))


def compute_txn_metrics(df: DataFrame, max_process_date: datetime.date, roll_back_days: int, group_keys: list) -> DataFrame:
    ref_col = f.lit(max_process_date) if max_process_date else f.current_date()

    past_90 = (f.col("process_date").between(f.date_sub(ref_col, roll_back_days - 1), ref_col))
    amt = f.coalesce(f.col("dw_net_pd_amt"), f.lit(0.0))
    cnt = f.coalesce(f.col("dw_net_pd_cnt"), f.lit(0))

    aggs = {
        "total_txn_value_past_90": f.sum(f.when(past_90, amt).otherwise(f.lit(0.0))),
        "total_txn_vol_past_90": f.sum(f.when(past_90, cnt).otherwise(f.lit(0))),
        "txn_amt_range_past_90": (f.max(f.when(past_90, amt)) - f.min(f.when(past_90, amt))),
        "distinct_product_cd_past_90": f.countDistinct(f.when(past_90, f.col("product_cd"))),
        "most_recent_txn_date": f.max("process_date"),
        "max_amt_90d": f.max(f.when(past_90, amt)),
        "min_amt_90d": f.min(f.when(past_90, amt)),
        "avg_tran_amt": f.round(f.when(f.sum(col("dw_net_pd_cnt")) != 0, f.sum(col("dw_net_pd_amt")) / f.sum(col("dw_net_pd_cnt"))).otherwise(0), 5),
        "matched_trans_count": f.sum("dw_net_pd_cnt"),
        "recent_comm_hist_txn_dt": f.max(f.when(col('product_cd') == 'MCP', f.col('process_date'))),
        "recent_in_crntl_hist_txn_dt": f.max(f.when(col('pds1_paypass_acct_nbr_type_ind').isin('8', '8C', '9', '9C'), f.col('process_date')))
    }

    return (df.groupBy(*group_keys).agg(*[expr.alias(name) for name, expr in aggs.items()])
            .withColumn("avg_daily_vol_past_90", f.col("total_txn_vol_past_90") / f.lit(90.0))
            .withColumn("days_since_most_recent_txn", f.datediff(ref_col, f.col("most_recent_txn_date"))))


def build_bins(df: DataFrame, metric: str, is_neg: bool, probs: list) -> DataFrame:
    q = df.select(f.col(f"{metric}__qs").alias("q"), f.lit(metric).alias("metric")) \
          .select("metric", f.posexplode("q").alias("idx", "qv"))

    n_bins = len(probs) - 1
    last_idx = n_bins - 1

    # take highs at idx=1..n_bins (skip 0 which is the min)
    hi = (q.filter((f.col("idx") >= 1) & (f.col("idx") <= n_bins))
          .select("metric", (f.col("idx") - 1).alias("bin_idx"), f.col("qv").cast("double").alias("hi")))

    w = W.partitionBy("metric").orderBy("bin_idx")
    bins = (hi
            .withColumn("prev_hi", f.lag("hi").over(w))
            .withColumn("prev_hi", f.when(f.col("bin_idx") == 0, f.lit(float("-inf"))).otherwise(f.col("prev_hi")))
            .withColumn("hi", f.when(f.col("bin_idx") == last_idx, f.lit(float("inf"))).otherwise(f.col("hi")))
            .withColumn("pct_lower", (f.col("bin_idx") / f.lit(n_bins)).cast("double"))
            .withColumn("pct_upper", ((f.col("bin_idx") + 1) / f.lit(n_bins)).cast("double"))
            .withColumn("is_negated", f.lit(is_neg))
            )
    return bins


def build_percentile_bins(df: DataFrame, higher: list, lower: list, args) -> DataFrame:
    probs = [i / 100 for i in range(0, 101)]

    dfq = df.select(*higher, *[(-f.col(c)).alias(f"neg__{c}") for c in lower])

    arr = []
    for c in higher:
        arr.append(f.expr(f"percentile_approx({c}, array({','.join(map(str, probs))}), 10000)").alias(f"{c}__qs"))
    for c in lower:
        arr.append(f.expr(f"percentile_approx(neg__{c}, array({','.join(map(str, probs))}), 10000)").alias(f"{c}__qs"))

    qs = dfq.groupBy().agg(*arr)

    bins_all = None
    for c in higher:
        b = build_bins(qs, c, is_neg=False, probs=probs)
        bins_all = b if bins_all is None else bins_all.unionByName(b, allowMissingColumns=True)
    for c in lower:
        b = build_bins(qs, c, is_neg=True, probs=probs)
        bins_all = bins_all.unionByName(b, allowMissingColumns=True)

    final_df = (bins_all
                .withColumn("generated_at", f.current_date())
                .withColumn("version", f.lit("v1"))
                )
    final_df.write.mode("overwrite").saveAsTable(args.bin_tbl)


def score_metric(df: DataFrame, bins: DataFrame, metrics: list) -> DataFrame:
    zero_trans_df = df.filter(col('total_txn_value_past_90') == 0)
    result_df = df.filter(col('total_txn_value_past_90') != 0)
    for metric in metrics:
        df_alias = result_df.alias("df")
        bins_m = bins.filter(f.col("metric") == metric).alias("bins")
        val = f.when(col("bins.is_negated"), -col(f"df.{metric}")).otherwise(col(f"df.{metric}"))
        in_range = (val > f.col("bins.prev_hi")) & (val <= f.col("bins.hi"))
        joined_df = df_alias.crossJoin(f.broadcast(bins_m)).where((in_range | f.col(f"df.{metric}").isNull()))

        pct = (f.when(f.col(f"df.{metric}").isNull(), f.lit(None).cast("double"))
               .otherwise((f.col("bins.pct_lower") + f.col("bins.pct_upper")) / 2.0))

        joined_df = (joined_df.withColumn(f"{metric}_percentile", f.round(pct, 2))
                     .drop("bin_idx", "lo", "hi", "pct_lower", "pct_upper", "is_negated", "generated_at", "version", "metric", "prev_hi"))

        zero_trans_df = zero_trans_df.withColumn(f"{metric}_percentile", f.lit(0.0).cast('double'))

        result_df = joined_df
    result_df = result_df.unionByName(zero_trans_df)

    return result_df


def get_propensity(df: DataFrame, args) -> DataFrame:  # pragma no cover
    model = mlflow.pyfunc.load_model(args.propensity_model_url)
    propensity_feature = [
        "total_txn_value_past_90_percentile",
        "total_txn_vol_past_90_percentile",
        "txn_amt_range_past_90_percentile",
        "distinct_product_cd_past_90_percentile",
        "days_since_most_recent_txn_percentile"]

    @pandas_udf(DoubleType())
    def predict_udf(*cols: pd.Series) -> pd.Series:
        input_df = pd.concat(cols, axis=1)
        input_df.columns = propensity_feature
        return pd.Series(model.predict(input_df))

    score_label_cond = f.when(col("propensity_score") > 0.7, "HIGH").when(col("propensity_score") > 0.6, "MEDIUM").otherwise("LOW")
    df = df.withColumn("propensity_score", predict_udf(*[df[pf] for pf in propensity_feature]))
    result_df = (df.withColumn("propensity_score", f.round("propensity_score", 2))
                 .withColumn("propensity_score_label", score_label_cond))
    return result_df


def get_quarter_from_date(dt):
    if dt is not None:
        return f"""{dt.year}_{int((dt.month - 1) / 3 + 1)}"""
    else:
        return None


def create_merchant_inference(spark: SparkSession, args):  # pragma no cover
    higher = ["avg_daily_vol_past_90", "total_txn_value_past_90", "total_txn_vol_past_90", "txn_amt_range_past_90", "distinct_product_cd_past_90"]
    lower = ["days_since_most_recent_txn"]

    latest_partition = datetime.strptime(
        str(spark.sql(f"""show partitions {args.sme_clring_tbl}""").select(f.max("process_date")).collect()[0][0]),
        "%Y-%m-%d").date()
    print(f"latest partition is {latest_partition}")
    df = get_merchant_transactions(spark, str(latest_partition), 365, args)
    df_agg = compute_txn_metrics(df, latest_partition, 90, ["entity_id", "dw_merch_region_cd"])

    if spark.sql(f"""show tables in {".".join(args.bin_tbl.split(".")[:2])} like '{args.bin_tbl.split(".")[-1]}'""").count() > 0:
        if spark.sql(f"""select count(*) from {args.bin_tbl}""").collect()[0][0] == 0:
            last_run_qr = None
            print("No data found in the bin table. Need to rebuild")
        else:
            last_run_qr = get_quarter_from_date(spark.sql(f"select distinct generated_at from {args.bin_tbl}").collect()[0][0])
        latest_run_qr = get_quarter_from_date(latest_partition - timedelta(days=1))
        if last_run_qr == latest_run_qr:
            print("skipped the run of percentile bins as quarter are same.......")
        else:
            print("rebuilding the percentile bins for the new quarter")
            build_percentile_bins(df_agg, higher, lower, args)
    else:
        print("building the percentile bins for the new quarter")
        build_percentile_bins(df_agg, higher, lower, args)

    bin_df = spark.read.table(args.bin_tbl)
    print("Generating the score metrics......")
    scored_df = score_metric(df_agg, bin_df, higher + lower)
    print("Getting the propensity score")
    propensity_df = get_propensity(scored_df.fillna(0), args)

    # recommendation_expr = f"{args.recommendation_func}(propensity_score, 90, min_amt_90d, max_amt_90d, entity_id)"
    recom_df = propensity_df.withColumn("suggested_talking_points", f.lit(None).cast('string'))

    # recom_df.write.mode("overwrite").saveAsTable(args.mer_summ_inference_tbl)
    table_name = args.mer_summ_inference_tbl
    refresh_date = date.today()
    # refresh_date = datetime.strptime("2026-03-11", "%Y-%m-%d").date()
    recom_df = recom_df.withColumn("refreshed_date", f.lit(refresh_date))
    recom_df.write.format("delta").mode("overwrite").partitionBy("refreshed_date", "dw_merch_region_cd").saveAsTable(table_name)

    print(f"{args.mer_summ_inference_tbl} table is created successfully....")


def create_merchant_summary_features(spark: SparkSession, args):
    cutc_rlto_join = (f.col('cutc.entity_id') == f.col('rlto.entity_id')) & (
        f.col('cutc.dw_merch_region_cd').cast('int') == f.col('rlto.region_code').cast('int'))

    df_rlto = spark.read.table(args.emd_flatt_tbl)

    df_mch = spark.sql(f"""
    SELECT DISTINCT
        merchant_category_code AS mcc_code,
        FIRST_VALUE(merchant_mcc_group_name) OVER (PARTITION BY merchant_category_code) AS mcc_group_name,
        FIRST_VALUE(classification_code) OVER (PARTITION BY merchant_category_code) AS classification_code
    FROM {args.member_catgry_hier_tbl} WHERE LEVEL_NUMBER = 20
""")

    # df_cutc = spark.read.table(args.mer_summ_inference_tbl)

    latest_refresh_date = spark.sql(f"select max(refreshed_date) from {args.mer_summ_inference_tbl}").collect()[0][0]
    df_cutc = spark.read.table(args.mer_summ_inference_tbl).filter(col("refreshed_date") == latest_refresh_date)

    df_joined = (df_cutc.alias('cutc').join(df_rlto.alias('rlto'), cutc_rlto_join, 'right')
                 .join(f.broadcast(df_mch.alias('mch')), ['mcc_code'], 'left'))

    df = df_joined.selectExpr(
        "rlto.entity_id AS entity_id",
        "rlto.url AS supplier_website_url",
        "rlto.region_name AS region",
        "rlto.mcc_code AS mcc_code",
        "rlto.dba_name AS supplier_alias_name",
        "mch.classification_code AS industry_classification_code",
        "'CARD' AS preferred_payment_method",
        "cutc.propensity_score AS propensity_score",
        "cast(int(months_between(current_date(), cutc.most_recent_txn_date)) as string) AS transaction_recency",
        "rlto.sic AS sic",
        "cutc.suggested_talking_points AS suggested_talking_points",
        "rlto.dba_name AS cleansed_supplier_name",
        "rlto.address_line1 AS cleansed_address_line_1",
        "rlto.city AS cleansed_city_name",
        "rlto.state_province AS Cleansed_state_Or_province_name",
        "rlto.country_cd AS cleansed_country_code",
        "rlto.postcode AS cleansed_postal_code",
        "rlto.aggr_merch_name AS aggregate_merchant_name",
        "rlto.aggr_merch_id AS aggregate_merchant_id",
        "rlto.parent_aggr_merch_name AS parent_aggregate_merchant_name",
        "rlto.parent_aggr_merch_id AS parent_aggregate_merchant_id",
        "mch.mcc_group_name as mcc_group",
        "rlto.region_name AS business_region_name",
        """CASE
            WHEN  cutc.most_recent_txn_date is NULL THEN NULL
            WHEN cutc.recent_comm_hist_txn_dt is NOT NULL THEN 'YES' ELSE 'NO'
        END AS commercial_history""",
        """CASE
            WHEN cutc.recent_comm_hist_txn_dt is NOT NULL THEN cast(months_between(current_date(), cutc.recent_comm_hist_txn_dt) as int)
            ELSE cast(NULL AS int)
        END AS commercial_recency""",
        "CAST(rlto.clearing_last_seen_date AS STRING) AS clearing_last_seen_date",
        "CAST(rlto.auth_last_seen_date AS STRING) AS auth_last_seen_date",
        "CAST(cutc.avg_tran_amt AS DOUBLE) AS avg_tran_amt",
        "cutc.matched_trans_count AS matched_trans_count",
        """CASE
            WHEN  cutc.most_recent_txn_date is NULL THEN NULL
            WHEN cutc.recent_in_crntl_hist_txn_dt IS NOT NULL THEN 'YES' ELSE 'NO'
        END AS in_control_history""",
        """CASE
            WHEN cutc.recent_in_crntl_hist_txn_dt IS NOT NULL THEN cast(months_between(current_date(), cutc.recent_in_crntl_hist_txn_dt) as int)
            ELSE cast(NULL AS int)
        END AS in_control_recency""",
        "CAST(rlto.zi_c_last_updated_date AS string) AS last_update_date",
        "rlto.naics AS customer_naics",
        "cutc.propensity_score_label AS propensity_score_label",
        "rlto.region_code AS dw_merch_region_cd",
        "rlto.industry AS industry",
        "rlto.mmh_id AS mmh_id"
    )
    crd_acceptor_cond = (f.size(f.split(col('mmh_id'), ",")) > 0) & (col('mmh_id') != '') & (col('mmh_id').isNotNull())
    df = (df.withColumn('mmh_id', f.concat_ws(",", "mmh_id"))
          .withColumn('card_acceptor', f.when(col('transaction_recency').isNull(), None).when(crd_acceptor_cond, 'YES').otherwise('NO'))
          )
    df.write.mode("overwrite").saveAsTable(args.mer_summ_feature_tbl, partitionBy=["dw_merch_region_cd"])
    print(f"{args.mer_summ_feature_tbl} table is created successfully....")


def parse_args(argv: List[str]) -> argparse.Namespace:  # pragma no cover
    p = argparse.ArgumentParser(description="Merchant features ETL")
    add = p.add_argument
    add('--merch_region_cd', default="01,05", help="merch_region_cd")
    add('--sme_clring_tbl', type=str, required=True, help="sme_clring_tbl")
    add('--mmh_loc_tbl', default="mc_sme.core.mmh_location", help="mmh_loc_tbl")
    add('--emd_flatt_tbl', default="mc_sme.bd.emd", help="emd_tbl")
    add('--bin_tbl', default="mc_sme.bd.so_bin_percentile_current_qr", help="bin_tbl")
    add('--mer_summ_inference_tbl', default="mc_sme.bd.so_merchant_summary_inference", required=True, help="mer_summ_inference_tbl")
    add('--mer_summ_feature_tbl', default="mc_sme.bd.so_merchant_summary_features", help="mer_summ_feature_tbl")
    add('--member_catgry_hier_tbl', default="mc_sme.core.merchant_category_hierarchy", help="member_catgry_hier_tbl")
    add('--propensity_model_url', default="runs:/b52b75626c614d17a67aead626c2e73b/model", help="propensity model url")
    return p.parse_args()


def main():  # pragma no cover
    args = parse_args(sys.argv[1:])
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("so_merchant_summary_feature").getOrCreate()
    create_merchant_inference(spark, args)
    create_merchant_summary_features(spark, args)


if __name__ == "__main__":  # pragma no cover
    main()
