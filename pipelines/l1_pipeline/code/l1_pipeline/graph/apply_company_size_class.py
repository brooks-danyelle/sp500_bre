from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from l1_pipeline.config.ConfigStore import *
from l1_pipeline.functions import *

def apply_company_size_class(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return add_rule(
        add_rule(
          add_rule(
            add_rule(add_rule(add_rule(in0, company_size_class()), composite_tier()), flag_investment_risk()),
            regional_classification()
          ),
          employee_scale()
        ),
        tech_subsector_class()
    )
