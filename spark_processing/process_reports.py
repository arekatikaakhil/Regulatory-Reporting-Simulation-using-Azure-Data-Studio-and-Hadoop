from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, expr

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RegulatoryReporting") \
    .getOrCreate()

# Base paths for input and output
base_path = "/Users/akhilarekatika/Work/Projects/RegulatoryReporting/simulated_hdfs/user/bank"
input_folder = f"{base_path}/raw_data"
output_folder = f"{base_path}/output_data"

# -----------------------------
# Load CSV files into DataFrames
# -----------------------------
accounts_df = spark.read.csv(f"{input_folder}/accounts.csv", header=True, inferSchema=True)
loans_df = spark.read.csv(f"{input_folder}/loans.csv", header=True, inferSchema=True)
liquidity_df = spark.read.csv(f"{input_folder}/liquidity.csv", header=True, inferSchema=True)

# -----------------------------
# Y-9C Report
# -----------------------------
y9c = accounts_df.groupBy("account_type") \
    .agg(sum("amount").alias("total_amount")) \
    .orderBy("total_amount", ascending=False)

y9c.write.mode("overwrite").option("header", True).csv(f"{output_folder}/Y9C_Summary")

# -----------------------------
# Y-14 Report
# -----------------------------
y14 = loans_df.groupBy("loan_type") \
    .agg(
        sum("balance").alias("total_balance"),
        avg("risk_weight").alias("avg_risk_weight"),
        expr("sum(balance * risk_weight)").alias("total_risk_weighted_assets")
    ) \
    .orderBy("total_risk_weighted_assets", ascending=False)

y14.write.mode("overwrite").option("header", True).csv(f"{output_folder}/Y14_Summary")

# -----------------------------
# 2052a Liquidity Report
# -----------------------------
fr2052a = liquidity_df.groupBy("source_type") \
    .agg(
        sum("cash_in").alias("total_cash_in"),
        sum("cash_out").alias("total_cash_out"),
        expr("sum(cash_in - cash_out)").alias("net_cash_flow")
    ) \
    .orderBy("net_cash_flow", ascending=False)

fr2052a.write.mode("overwrite").option("header", True).csv(f"{output_folder}/FR2052a_Summary")

print("✅ Reports generated successfully in the output folder.")

# Stop Spark session
spark.stop()
