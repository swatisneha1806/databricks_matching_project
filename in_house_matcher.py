import argparse
import sys
from typing import List

from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.functions import array_to_vector
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, greatest, when, lit, row_number, coalesce
from pyspark.sql.types import StringType, DoubleType
import pyspark.sql.functions as f
import string


REQUIRED_COLS_FOR_MATCHING = ["supplier_id", "supplier_name", "state", "postal_code", "country", "street_address"]


BASE62_CHARS = string.digits + string.ascii_uppercase + string.ascii_lowercase


def to_base62_8(num: int) -> str:
    """Convert a numeric string to an 8-character base62 encoded string."""
    try:
        (base, chars) = (62, [])
        n = num
        while n > 0:
            n, rem = divmod(n, base)
            chars.append(BASE62_CHARS[rem])
        # reverse & pad to exactly 8 chars (or trim if slightly longer)
        encoded = ''.join(reversed(chars))
        return encoded.rjust(8, '0')[-8:]
    except (ValueError, TypeError):
        return ""


to_base62_udf = f.udf(to_base62_8, StringType())


def cosine_similarity(a, b):
    dot = F.aggregate(F.zip_with(a, b, lambda x, y: x * y), F.lit(0.0), lambda acc, v: acc + v)
    norm_a = F.sqrt(F.aggregate(F.transform(a, lambda x: x * x), F.lit(0.0), lambda acc, v: acc + v))
    norm_b = F.sqrt(F.aggregate(F.transform(b, lambda x: x * x), F.lit(0.0), lambda acc, v: acc + v))
    return (F.when(a.isNull() | b.isNull(), F.lit(None).cast(DoubleType()))
            .otherwise(F.when((norm_a == 0) | (norm_b == 0), F.lit(0.0)).otherwise(dot / (norm_a * norm_b))))


def normalize(c):
    c2 = F.coalesce(c, F.lit(""))
    return F.trim(F.regexp_replace(F.regexp_replace(F.lower(c2), r'[^a-z0-9\s]', ''), r'\s+', ' '))


def generate_embeddings(input_df: DataFrame):
    df = (input_df.withColumn("loc_id", col("old_loc_id").cast(StringType()))
          .withColumn("entity_id", to_base62_udf(f.col("merchant_market_hierarchy_id")))
          .withColumn("legal_name", coalesce(col("cleansed_legal_corporate_name"), col("cleansed_merchant_name")))
          .withColumnRenamed("cleansed_merchant_name", "merchant_name")
          .withColumn("street_addr", normalize("cleansed_merchant_street_addr"))
          .withColumn("state", normalize("cleansed_state_province_code"))
          .withColumn("country", normalize("cleansed_country_code"))
          .withColumnRenamed("cleansed_merchant_postal_code", "postal_code")
          .withColumn("ln_emb", F.expr("ai_query('databricks-gte-large-en', legal_name)"))
          .withColumn("mn_emb", F.expr("ai_query('databricks-gte-large-en', merchant_name)"))
          .select("loc_id", "entity_id", "legal_name", "merchant_name", "street_addr", "postal_code", "state", "country", "ln_emb", "mn_emb"))
    return df


def process_source(src_df: DataFrame, model_path: str, output_tbl: str, buckets):

    src_vectors = (src_df.withColumn("legal_name_vector", array_to_vector(col("ln_emb")))
                   .withColumn("merchant_name_vector", array_to_vector(col("mn_emb"))))

    legal_embeddings = src_vectors.select(col("loc_id"), col("legal_name").alias("name"), col("legal_name_vector").alias("features"), lit("legal_name").alias("embedding_type"))
    merchant_embeddings = src_vectors.select(col("loc_id"), col("merchant_name").alias("name"), col("merchant_name_vector").alias("features"), lit("merchant_name").alias("embedding_type"))
    all_embeddings = legal_embeddings.union(merchant_embeddings).filter(col("features").isNotNull())

    kmeans = KMeans(k=buckets, seed=42, maxIter=20, featuresCol="features", predictionCol="cluster_id")
    kmeans_model = kmeans.fit(all_embeddings)
    kmeans_model.write().overwrite().save(model_path)

    legal_with_clusters = kmeans_model.transform(
        src_vectors.select("loc_id", "legal_name", "legal_name_vector")
        .withColumnRenamed("legal_name_vector", "features")
    ).select(col("loc_id"), col("cluster_id").alias("legal_cluster_id"))

    merchant_with_clusters = kmeans_model.transform(
        src_vectors.select("loc_id", "merchant_name", "merchant_name_vector")
        .withColumnRenamed("merchant_name_vector", "features")
    ).select(col("loc_id"), col("cluster_id").alias("merchant_cluster_id"))

    src_with_clusters = src_vectors.join(legal_with_clusters, "loc_id", "left").join(merchant_with_clusters, "loc_id", "left")
    src_with_clusters.write.mode("overwrite").partitionBy("country").saveAsTable(output_tbl)


def match(spark: SparkSession, model_path: str, src_tbl: str, input_df: DataFrame, audit_table: str):
    # Validate columns
    missing = [c for c in REQUIRED_COLS_FOR_MATCHING if c not in input_df.columns]
    if missing:
        raise ValueError(f"Input DataFrame missing columns: {missing}")

    countries = [row["country"] for row in input_df.select("country").distinct().collect()]
    results = []
    for country in countries:
        batch_df = input_df.filter(col("country") == country)
        result = match_with_audit(spark, model_path, src_tbl, batch_df, audit_table)
        results.append(result)
    final_result = results[0]
    for r in results[1:]:
        final_result = final_result.unionByName(r)
    return final_result


def match_with_audit(spark: SparkSession, model_path: str, src_tbl: str, input_df: DataFrame, audit_table: str):
    audit_df = spark.table(audit_table)
    # Preserve original supplier_id mapping
    raw_input = input_df.select("supplier_id", "supplier_name", "street_address", "country").dropDuplicates()

    # Ensure required cols for match (add missing with nulls)
    if "postal_code" not in input_df.columns:
        input_df = input_df.withColumn("postal_code", F.lit(None).cast(StringType()))
    if "state" not in input_df.columns:
        input_df = input_df.withColumn("state", F.lit(None).cast(StringType()))

    # Normalize + embedding
    norm_df = (input_df
               .withColumn("add_norm", normalize("street_address"))
               .withColumn("name_norm", normalize("supplier_name"))
               .withColumn("supplier_name_norm", normalize(F.concat_ws(" ", F.col("name_norm"), F.col("add_norm"))))
               .withColumn("in_emb", F.expr("ai_query('databricks-gte-large-en', supplier_name_norm)"))
               .select("supplier_id", "supplier_name", "street_address", "postal_code", "state", "country", "in_emb"))

    join_keys = ["supplier_name", "street_address", "country"]
    incoming_keys = norm_df.select(*join_keys).distinct()
    audited_keys = audit_df.select(*join_keys).distinct()
    to_match_keys = incoming_keys.join(audited_keys, join_keys, "left_anti")
    to_match_df = norm_df.join(to_match_keys, join_keys, "inner")

    if to_match_df.limit(1).count() > 0:
        print("finding matches...")
        new_matches = find_best_match(model_path, spark.table(src_tbl), to_match_df)
        # Reduce to audit schema
        new_audit_rows = new_matches.select(*join_keys, col("entity_id"), col("is_matched"), col("matched_score"))
        new_audit_rows.write.mode("append").saveAsTable(audit_table)
        audit_df = audit_df.unionByName(new_audit_rows)
    # Enrich audit with supplier_id(s) from current request
    result = (raw_input
              .join(audit_df, join_keys, "left")
              .withColumn("is_matched", F.coalesce(col("is_matched"), F.lit("N")))
              .withColumn("matched_score", F.coalesce(col("matched_score"), F.lit(0.0)))
              .select("supplier_id", "entity_id", "is_matched", "matched_score"))

    return result


def find_best_match(model_path: str, src_with_clusters: DataFrame, input_df: DataFrame, similarity_threshold: float = 0.60, top_n: int = 1) -> DataFrame:
    input_vectors = input_df.withColumn("in_vector", array_to_vector(col("in_emb")))

    kmeans_model = KMeansModel.load(model_path)
    input_with_clusters = (kmeans_model.transform(input_vectors.withColumnRenamed("in_vector", "features"))
                           .select(col("supplier_id"), col("supplier_name"), col("street_address"), col("country"), col("in_emb"), col("cluster_id").alias("input_cluster_id")))

    matches = input_with_clusters.alias("inp").join(
        src_with_clusters.alias("src"),
        (col("inp.input_cluster_id") == col("src.legal_cluster_id")) | (col("inp.input_cluster_id") == col("src.merchant_cluster_id")))

    scores = (matches.withColumn("legal_name_score", cosine_similarity(col("inp.in_emb"), col("src.ln_emb")))
              .withColumn("merchant_name_score", cosine_similarity(col("inp.in_emb"), col("src.mn_emb")))
              .withColumn("matched_score", greatest(col("legal_name_score"), col("merchant_name_score")))
              .withColumn("best_match_type", when(col("legal_name_score") > col("merchant_name_score"), "legal_name").otherwise("merchant_name"))
              .withColumn("best_match_name", when(col("legal_name_score") > col("merchant_name_score"), col("src.legal_name")).otherwise(col("src.merchant_name"))))

    matches_with_best = scores.filter(col("matched_score") >= lit(similarity_threshold))
    window_spec = Window.partitionBy("inp.supplier_id").orderBy(col("matched_score").desc())
    best_matches = matches_with_best.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == top_n)
    best_matches = best_matches.select(col("inp.supplier_id"), col("inp.supplier_name"), col("inp.street_address"),
                                       col("inp.country"), col("src.entity_id"), lit("Y").alias("is_matched"), col("matched_score"))
    return best_matches


def parse_args(argv: List[str]):  # pragma no cover
    parser = argparse.ArgumentParser(description="in house matching")
    parser.add_argument("--merch_region_cd", default="01", help="merch_region_cd")
    parser.add_argument("--input_table", type=str, required=False, help="input table name to match")
    parser.add_argument("--output_table", type=str, required=False, help="matched result output table name")
    parser.add_argument("--source_table", type=str, required=False, help="source table name")
    parser.add_argument("--source_emb_table", type=str, required=False, help="source embedding table name")
    parser.add_argument("--clustered_table", type=str, required=False, help="clustered/bucketed table name")
    parser.add_argument("--model_path", type=str, required=False, help="model path")
    parser.add_argument("--audit_table", type=str, required=False, help="audit table name to record matched results")
    parser.add_argument('--top_n', type=int, required=False, default=1, help="top n matches to return")
    parser.add_argument('--buckets', type=int, required=False, default=100, help="top n matches to return")
    parser.add_argument('--similarity_threshold', type=float, required=False, default=0.60, help="similarity threshold")
    parser.add_argument("--action", type=str, required=True, help="either 'matching' or 'source_embedding' or 'build_model'")
    return parser.parse_args(argv)


def main():  # pragma: no cover
    args = parse_args(sys.argv[1:])
    print(f"Job started with args: {args}")

    spark = SparkSession.getActiveSession() or SparkSession.builder.appName("In_House Matching").getOrCreate()

    if args.action == "source_embedding":
        region_list_sql = ",".join(f"'{r.strip()}'" for r in args.merch_region_cd.split(",") if r.strip())
        emb_df = generate_embeddings(spark.table(args.source_table).where(f"business_region_code in ({region_list_sql})"))
        print("saving source embeddings to table:", args.source_emb_table)
        emb_df.write.mode("append").partitionBy("country").saveAsTable(args.source_emb_table)
    elif args.action == "build_model":
        process_source(spark.table(args.source_emb_table), args.model_path, args.clustered_table, args.buckets)
    elif args.action == "matching":
        input_df = spark.table(args.input_table)
        best_matches = match(spark, args.model_path, args.clustered_table, input_df, args.audit_table)
        best_matches.write.mode("overwrite").saveAsTable(args.output_table)
    else:
        raise ValueError(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
