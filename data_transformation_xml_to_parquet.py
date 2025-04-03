from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame

datasource = glueContext.create_dynamic_frame.from_catalog(
    database="production_logs",
    table_name="xml_data"
)

# Resolve schema inconsistencies
resolved = ResolveChoice.apply(datasource, choice="MATCH_CATALOG")

# Partition data by location and date
partitioned = datasource.repartition(100).toDF().write.partitionBy("factory_location", "production_date").parquet("s3://output-bucket/partitioned-data/")
