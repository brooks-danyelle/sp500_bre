from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_pipeline.config.ConfigStore import *
from l1_pipeline.functions import *

def finance_sp_500_companies(spark: SparkSession) -> DataFrame:
    return spark.read.table("`danyelle`.`finance`.`sp_500_companies`")
