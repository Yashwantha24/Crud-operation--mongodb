"""
Customer Orders Analytics - PySpark mini project

This script demonstrates:
- Loading JSON and CSV into DataFrames
- Basic schema cleanup and casting
- Transformations (filters, withColumn, drop)
- Actions (show, count, take)
- Aggregations and joins
- Writing outputs to CSV and Parquet
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    """Create or get a SparkSession with sensible defaults for local dev."""
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
    )


def load_dataframes(spark: SparkSession, base_path: Path):
    """Load customers JSON and orders CSV into DataFrames with explicit schemas."""
    customers_schema = T.StructType(
        [
            T.StructField("customer_id", T.IntegerType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("signup_date", T.StringType(), True),
        ]
    )
    orders_schema = T.StructType(
        [
            T.StructField("order_id", T.IntegerType(), False),
            T.StructField("customer_id", T.IntegerType(), False),
            T.StructField("order_date", T.StringType(), True),
            T.StructField("total_amount", T.DoubleType(), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
        ]
    )

    # Load customers from JSON (creates a DataFrame)
    customers_df = (
        spark.read.schema(customers_schema)
        .json(str(base_path / "data" / "customers.json"))
        .withColumn("signup_date", F.to_date("signup_date"))
    )

    # Load orders from CSV (creates a DataFrame)
    orders_df = (
        spark.read.schema(orders_schema)
        .option("header", True)
        .csv(str(base_path / "data" / "orders.csv"))
        .withColumn("order_date", F.to_date("order_date"))
    )

    return customers_df, orders_df


def transform(customers_df, orders_df):
    """Apply sample transformations, filters, aggregations, and joins."""
    # Filter orders: keep only shipped orders with positive amounts
    shipped_orders = orders_df.filter(
        (F.col("status") == F.lit("SHIPPED")) & (F.col("total_amount") > 0)
    )

    # Add a USD-normalized amount column (pretend conversion; here 1:1 for simplicity)
    shipped_orders = shipped_orders.withColumn("amount_usd", F.col("total_amount"))

    # Aggregate totals per customer
    totals_by_customer = (
        shipped_orders.groupBy("customer_id")
        .agg(
            F.sum("amount_usd").alias("total_amount"),
            F.count("*").alias("order_count"),
        )
    )

    # Join customers with their totals (left join keeps customers with no orders)
    customers_enriched = (
        customers_df.join(totals_by_customer, on="customer_id", how="left")
        .fillna({"total_amount": 0.0, "order_count": 0})
    )

    # Aggregate totals by country for a simple analytics view
    totals_by_country = (
        customers_enriched.groupBy("country")
        .agg(
            F.sum("total_amount").alias("total_amount"),
            F.sum("order_count").alias("order_count"),
        )
        .orderBy(F.desc("total_amount"))
    )

    return customers_enriched, totals_by_country, shipped_orders


def write_outputs(customers_enriched, totals_by_country, shipped_orders, output_base: Path):
    """Write DataFrames to Parquet/CSV and small text files."""
    output_base.mkdir(parents=True, exist_ok=True)

    # Write DataFrame to Parquet (action: triggers computation)
    # Workaround for Windows: fallback to pandas if Spark Parquet write fails
    try:
        customers_enriched.coalesce(1).write.mode("overwrite").parquet(str(output_base / "customers_enriched.parquet"))
    except Exception as e:
        # Fallback: use pandas to write Parquet (Windows workaround)
        print(f"Warning: Spark Parquet write failed ({type(e).__name__}), using pandas fallback...")
        import pandas as pd
        parquet_path = output_base / "customers_enriched.parquet"
        parquet_path.mkdir(parents=True, exist_ok=True)
        pdf = customers_enriched.toPandas()
        pdf.to_parquet(str(parquet_path / "part-00000.parquet"), engine='pyarrow', index=False)

    # Write aggregated totals to CSV with header (action)
    try:
        totals_by_country.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_base / "orders_by_country.csv"))
    except Exception as e:
        # Fallback: use pandas to write CSV (Windows workaround)
        print(f"Warning: Spark CSV write failed ({type(e).__name__}), using pandas fallback...")
        import pandas as pd
        csv_path = output_base / "orders_by_country.csv"
        csv_path.mkdir(parents=True, exist_ok=True)
        pdf = totals_by_country.toPandas()
        pdf.to_csv(str(csv_path / "part-00000.csv"), index=False)

    # Example action: collect basic counts and write to a small text file
    counts_text = "\n".join(
        [
            f"customers_enriched count: {customers_enriched.count()}",
            f"orders (shipped) count: {shipped_orders.count()}",
            f"country groups: {totals_by_country.count()}",
        ]
    )
    (output_base / "sample_action_counts.txt").write_text(counts_text, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Run the Customer Orders Analytics PySpark job.")
    parser.add_argument("--master", default="local[*]", help="Spark master URL (default: local[*])")
    parser.add_argument("--output-base", default="output", help="Output directory (default: output)")
    args = parser.parse_args()

    base_path = Path(__file__).resolve().parent.parent
    output_base = base_path / args.output_base

    # Initialize SparkSession
    spark = build_spark(app_name="CustomerOrdersAnalytics", master=args.master)

    try:
        # Load input data
        customers_df, orders_df = load_dataframes(spark, base_path)

        # Inspect schemas and sample rows (actions: print to console)
        customers_df.printSchema()
        orders_df.printSchema()
        customers_df.show(5, truncate=False)
        orders_df.show(5, truncate=False)

        # Transform data
        customers_enriched, totals_by_country, shipped_orders = transform(customers_df, orders_df)

        # Display transformation results (actions)
        customers_enriched.orderBy(F.desc("total_amount")).show(10, truncate=False)
        totals_by_country.show(truncate=False)

        # Persist outputs
        write_outputs(customers_enriched, totals_by_country, shipped_orders, output_base)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

