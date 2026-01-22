from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *

@execute_rule
def company_size_class(marketcap: Column=lambda: col("Marketcap")):
    return when((marketcap >= lit(200000000000)), lit("mega cap"))\
        .when(((marketcap >= lit(10000000000)) & (marketcap <= lit(199999999999))), lit("large cap"))\
        .when((marketcap < lit(10000000000)), lit("mid cap"))\
        .otherwise(lit(None))\
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

@execute_rule
def regional_classification(state: Column=lambda: col("State")):
    return when(state.isin(lit("CA"), lit("WA"), lit("OR")), lit("West Coast"))\
        .when(state.isin(lit("NY"), lit("MA"), lit("NJ"), lit("CT"), lit("PA")), lit("Northeast"))\
        .when(state.isin(lit("IL"), lit("OH"), lit("MI"), lit("MN"), lit("WI")), lit("Midwest"))\
        .when(state.isin(lit("TX"), lit("FL"), lit("GA"), lit("NC"), lit("VA")), lit("South"))\
        .otherwise(lit("other"))\
        .alias("Region")

@execute_rule
def tech_subsector_class(sector: Column=lambda: col("Sector"), industry: Column=lambda: col("Industry")):
    return when(((sector == lit("Technology")) & (industry == lit("Semiconductors"))), lit("Semiconductors"))\
        .when(((sector == lit("Technology")) & industry.like("%Software%")), lit("Software"))\
        .when(
          ((sector == lit("Technology")) & industry.isin(lit("Consumer Electronics"), lit("Computer Hardware"))), 
          lit("Hardware")
        )\
        .when(((sector == lit("Technology")) & industry.like("%Services%")), lit("Services"))\
        .when((sector == lit("Technology")), lit("Other Tech"))\
        .otherwise(lit("Not Tech"))\
        .alias("Subsector")
