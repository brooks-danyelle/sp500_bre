from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from l1_pipeline.config.ConfigStore import *
from l1_pipeline.functions import *
from prophecy.utils import *
from l1_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_finance_silver_sp_500_stock_daily = finance_silver_sp_500_stock_daily(spark)
    df_finance_sp_500_companies_1 = finance_sp_500_companies_1(spark)
    df_by_symbol = by_symbol(spark, df_finance_silver_sp_500_stock_daily, df_finance_sp_500_companies_1)
    df_apply_company_size_class = apply_company_size_class(spark, df_by_symbol)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("l1_pipeline").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/l1_pipeline")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/l1_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
