from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *

@execute_rule
def company_size_class(marketcap: Column=lambda: col("Marketcap")):
    return when((marketcap >= lit(200000000000)), lit("mega cap"))\
        .when(((marketcap >= lit(10000000000)) & (marketcap <= lit(199999999999))), lit("large cap"))\
        .when((marketcap < lit(10000000000)), lit("mid cap"))\
        .alias("cap_value")

@execute_rule
def flag_investment_risk(
        marketcap: Column=lambda: col("Marketcap"), 
        revenuegrowth: Column=lambda: col("Revenuegrowth"), 
        ebitda: Column=lambda: col("Ebitda")
):
    return when((((marketcap >= lit(100000000000)) & (revenuegrowth >= lit(0))) & (ebitda > lit(0))), lit("Low Risk"))\
        .when(((marketcap >= lit(10000000000)) & (revenuegrowth >= lit(-0.1))), lit("Medium Risk"))\
        .when((revenuegrowth < lit(-0.1)), lit("High Risk"))\
        .when((ebitda <= lit(0)), lit("High Risk"))\
        .otherwise(lit("Medium Risk"))\
        .alias("Investment_risk_flag")
