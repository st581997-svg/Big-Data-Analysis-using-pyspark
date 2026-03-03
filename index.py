#1. BASIC SETUP

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("BankLoanAnalysis").getOrCreate()

df = spark.read.csv(
    "C:/DATA SETS/bank_personal_loan_data2.csv",
    header=True,
    inferSchema=True
)
#2.BASIC DATESET ANALYSIS


df.printSchema()
df.show(5)

print("Total Records:",df.count())

print("Columns:",df.columns)

df.describe().show()

#3. LOAN APPOROVEL COUNT 

df.groupBy("loan_status").count().show()

df.groupBy("loan_status")\
.agg(avg("income").alias("avg_income"))\
.show()

# 4.AVERAGE CREDIT SCORE BY LOAN STATUS

df.groupBy("loan_status")\
.agg(avg("credit_score").alias("avg_credit_score"))\
.show()

# HIGH CREDIT SCORE CUSTOMERS(ABOVE 750)

high_credit = df.filter(col("credit_score") > 750)
high_credit.show()

# CUSTOMERS WITH INCOME > 50 

df.filter(col("income") > 50).show()

#4.ADD NEW CALCULATED COLUMN (FEATURE ENGINEERING)

df = df.withColumn(
    "income_to_loan_ratio",
    col("loan_amount") / col("income")
)
df.show(5)

# RISK CATEGORY BASED ONCREDIT SCORE 

df = df.withColumn(
    "risk_category",
    when(col("credit_score")>= 750,"Low Risk")
    .when((col("credit_score")>= 650) & (col("credit_score") < 750), "Medium Risk")
    .otherwise("High Risk")
)

df.select("customer_id","credit_score","risk_category").show()

# 5.SORT DATA

# TOP 10 HIGHEST INCOME CUSTOMERS

df.orderBy(col("income").desc()).show(10)

# LOWER CREDIT SCORE CUSTOMERS 

df.orderBy(col("credit_score")).show(5)

#6. SQL METHOD 

df.createOrReplaceTempView("loans")

spark.sql("""
          SELECT loan_status,
          COUNT(*) as total_customers,
          AVG(income) as avg_income,
          AVG(credit_score) as avg_credit
          FROM loans
          GROUP BY loan_status""").show()

#7 SAVE PROCESSED DATA 

df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", True) \
  .csv("C:/DATA SETS/output_loans")
