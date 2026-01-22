from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_pipeline.config.ConfigStore import *
from l1_pipeline.functions import *

def by_symbol(
        spark: SparkSession,
        finance_silver_sp_500_stock_daily: DataFrame,
        finance_sp_500_companies_1: DataFrame,
) -> DataFrame:
    return finance_silver_sp_500_stock_daily\
        .alias("finance_silver_sp_500_stock_daily")\
        .join(
          finance_sp_500_companies_1.alias("finance_sp_500_companies_1"),
          (col("finance_silver_sp_500_stock_daily.symbol") == col("finance_sp_500_companies_1.Symbol")),
          "inner"
        )\
        .select(col("finance_sp_500_companies_1.Exchange").alias("Exchange"), col("finance_sp_500_companies_1.Shortname").alias("Shortname"), col("finance_sp_500_companies_1.Longname").alias("Longname"), col("finance_sp_500_companies_1.Sector").alias("Sector"), col("finance_sp_500_companies_1.Industry").alias("Industry"), col("finance_sp_500_companies_1.Currentprice").alias("Currentprice"), col("finance_sp_500_companies_1.Marketcap").alias("Marketcap"), col("finance_sp_500_companies_1.Ebitda").alias("Ebitda"), col("finance_sp_500_companies_1.Revenuegrowth").alias("Revenuegrowth"), col("finance_sp_500_companies_1.City").alias("City"), col("finance_sp_500_companies_1.State").alias("State"), col("finance_sp_500_companies_1.Country").alias("Country"), col("finance_sp_500_companies_1.Fulltimeemployees").alias("Fulltimeemployees"), col("finance_sp_500_companies_1.Longbusinesssummary").alias("Longbusinesssummary"), col("finance_sp_500_companies_1.Weight").alias("Weight"), col("finance_silver_sp_500_stock_daily.high").alias("high"), col("finance_silver_sp_500_stock_daily.adj_close").alias("adj_close"), col("finance_silver_sp_500_stock_daily.open").alias("open"), col("finance_silver_sp_500_stock_daily.volume").alias("volume"), col("finance_silver_sp_500_stock_daily.low").alias("low"), col("finance_silver_sp_500_stock_daily.symbol").alias("symbol"), col("finance_silver_sp_500_stock_daily.date").alias("date"), col("finance_silver_sp_500_stock_daily.close").alias("close"), col("finance_silver_sp_500_stock_daily.ingested_at").alias("ingested_at"))
