from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import (
    col,
    lit,
    when,
    count,
    sum as _sum,
    expr, 
    coalesce,
    broadcast,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    DoubleType,
    LongType,
    ArrayType,
)
import time

MONGO_URI = "mongodb://localhost:27017/ZOV?replicaSet=rs0"
S3_BUCKET_WAREHOUSE = "s3a://zov-ta4ilka/iceberg_warehouse"

USERS_COLLECTION = "users"
REGIONS_COLLECTION = "regions"

ICEBERG_TABLE_NAME = "analytics.mortality_report_streaming" 
CHECKPOINT_LOCATION = f"{S3_BUCKET_WAREHOUSE}/_checkpoints/mortality_analysis_streaming"


USER_SCHEMA = StructType(
    [
        StructField("_id", StringType(), True), 
        StructField("id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("fullName", StringType(), True),
        StructField("socialRating", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField(
            "currentLocation",
            StructType(
                [
                    StructField(
                        "position",
                        StructType(
                            [
                                StructField("type", StringType(), True),
                                StructField(
                                    "coordinates", ArrayType(DoubleType()), True
                                ),
                            ]
                        ),
                        True,
                    ),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField("regionId", StringType(), True),
        StructField("districtId", StringType(), True),
        StructField("countryId", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("lastLocationUpdateTimestamp", LongType(), True),
    ]
)

REGION_SCHEMA = StructType(
    [
        StructField("_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("parentRegionId", StringType(), True),
        StructField(
            "boundaries",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField(
                        "coordinates",
                        ArrayType(ArrayType(ArrayType(DoubleType()))),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("averageSocialRating", DoubleType(), True),
        StructField("populationCount", LongType(), True),
        StructField("importantPersonsCount", LongType(), True),
        StructField("underThreat", BooleanType(), True),
    ]
)

def get_spark_session():
    """Инициализирует и возвращает SparkSession с поддержкой Iceberg и S3 для стриминга."""
    
    parsed_db_name_for_config = ""
    if MONGO_URI:
        base_uri = MONGO_URI.split("?")[0]
        uri_parts = base_uri.split('/')
        if len(uri_parts) > 3:
            if uri_parts[-1]:
                parsed_db_name_for_config = uri_parts[-1]
            elif len(uri_parts) > 4 and uri_parts[-2]:
                 parsed_db_name_for_config = uri_parts[-2]

    if not parsed_db_name_for_config:
        print(f"Warning: Could not parse database name from MONGO_URI ('{MONGO_URI}').")

    print(f"Configuring Spark. MONGO_URI: '{MONGO_URI}', DB for config: '{parsed_db_name_for_config}'")
    
    spark_builder = (
        SparkSession.builder.appName("MongoDB_Iceberg_Streaming_Analytics")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", S3_BUCKET_WAREHOUSE)
        .config("spark.mongodb.input.uri", MONGO_URI) 
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net") 
        .config("spark.hadoop.fs.s3a.region", "ru-central1") 
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
    )

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN") 
    return spark

def process_batch(users_micro_batch_df, batch_id, regions_df_static_snapshot, full_iceberg_table_name, actual_regions_for_merge_df):
    spark = users_micro_batch_df.sparkSession
    print(f"--- Processing Batch ID: {batch_id} ---")

    if not users_micro_batch_df.head(1):
        print(f"Batch {batch_id}: Empty micro-batch of user changes. Skipping.")
        print(f"--- Finished Batch ID: {batch_id} (empty user changes) ---")
        return

    print(f"Batch {batch_id}: Received non-empty micro-batch of user changes.")

    if not regions_df_static_snapshot.head(1):
        print(f"Batch {batch_id}: Static regions data is empty. Cannot process users effectively. Displaying current table stats.")
        display_current_statistics(spark, full_iceberg_table_name, batch_id)
        print(f"--- Finished Batch ID: {batch_id} (no regions data) ---")
        return
    
    actual_regions_df = regions_df_static_snapshot.filter(col("region_type") == lit("REGION")).alias("actual_regions")
    cities_df = regions_df_static_snapshot.filter(col("region_type") == lit("CITY")).alias("cities")
    districts_df = regions_df_static_snapshot.filter(col("region_type") == lit("DISTRICT")).alias("districts")

    current_users_df = users_micro_batch_df.alias("current_users_batch")

    try:
        mapped_users_from_district = current_users_df.filter(col("districtId").isNotNull()).join(
            broadcast(districts_df), col("current_users_batch.districtId") == col("districts.region_doc_id"), "inner"
        ).join(
            broadcast(cities_df), col("districts.parentRegionId") == col("cities.region_doc_id"), "inner"
        ).join(
            broadcast(actual_regions_df), col("cities.parentRegionId") == col("actual_regions.region_doc_id"), "inner"
        ).select(
            col("current_users_batch.user_doc_id"),
            col("current_users_batch.active"),
            col("actual_regions.region_doc_id").alias("effective_region_id"),
            col("actual_regions.region_name").alias("effective_region_name")
        )

        mapped_users_from_city = current_users_df.filter(
            col("regionId").isNotNull() & col("districtId").isNull()
        ).join(
            broadcast(cities_df), col("current_users_batch.regionId") == col("cities.region_doc_id"), "inner"
        ).join(
            broadcast(actual_regions_df), col("cities.parentRegionId") == col("actual_regions.region_doc_id"), "inner"
        ).select(
            col("current_users_batch.user_doc_id"),
            col("current_users_batch.active"),
            col("actual_regions.region_doc_id").alias("effective_region_id"),
            col("actual_regions.region_name").alias("effective_region_name")
        )

        mapped_users_direct_to_region = current_users_df.filter(
            col("regionId").isNotNull() & col("districtId").isNull()
        ).join(
            broadcast(actual_regions_df), col("current_users_batch.regionId") == col("actual_regions.region_doc_id"), "inner"
        ).select(
            col("current_users_batch.user_doc_id"),
            col("current_users_batch.active"),
            col("actual_regions.region_doc_id").alias("effective_region_id"),
            col("actual_regions.region_name").alias("effective_region_name")
        )
        
        all_users_mapped_to_final_region = mapped_users_from_district.unionByName(
            mapped_users_from_city
        ).unionByName(
            mapped_users_direct_to_region
        ).distinct()

        if not all_users_mapped_to_final_region.head(1):
            print(f"Batch {batch_id}: No users mapped to a final region in this micro-batch after joins. Skipping further processing.")
            # No deltas to merge, but we will still display current statistics later
        else:
            print(f"Batch {batch_id}: Some users were mapped to final regions. Proceeding with delta calculation.")
        
            total_population_change_in_batch = all_users_mapped_to_final_region.groupBy(
                "effective_region_id", "effective_region_name"
            ).agg(count("user_doc_id").alias("total_population_change"))

            dead_population_change_in_batch = (
                all_users_mapped_to_final_region.filter(col("active") == lit(False))
                .groupBy("effective_region_id", "effective_region_name")
                .agg(count("user_doc_id").alias("dead_population_change"))
            )

            print(f"Batch {batch_id}: Calculating population changes (deltas) in this batch.")
            summary_deltas = total_population_change_in_batch.join(
                    dead_population_change_in_batch,
                    ["effective_region_id", "effective_region_name"],
                    "outer"
                ).select(
                    col("effective_region_id"),
                    col("effective_region_name"),
                    coalesce(col("total_population_change"), lit(0)).alias("total_population_change"),
                    coalesce(col("dead_population_change"), lit(0)).alias("dead_population_change")
                )
            
            if summary_deltas.head(1):
                print(f"Batch {batch_id}: Calculated population deltas. Preparing for MERGE into {full_iceberg_table_name}")
                
                deltas_with_parent_info = summary_deltas.join(
                    actual_regions_for_merge_df.select(
                        col("region_doc_id").alias("effective_region_id"),
                        col("parentRegionId").alias("parent_country_id_source")
                    ),
                    ["effective_region_id"],
                    "left"
                )

                deltas_with_parent_info.createOrReplaceTempView("source_deltas")
                
                merge_sql = f"""
                MERGE INTO {full_iceberg_table_name} AS target
                USING source_deltas AS source
                ON target.region_id = source.effective_region_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.dead_population = target.dead_population + source.dead_population_change,
                        target.mortality_rate_percent = 
                            CASE 
                                WHEN target.total_population > 0 
                                THEN ((target.dead_population + source.dead_population_change) * 100.0) / target.total_population
                                ELSE 0.0 
                            END
                WHEN NOT MATCHED THEN
                    INSERT (region_id, region_name, region_type, parent_country_id, total_population, dead_population, mortality_rate_percent)
                    VALUES (
                        source.effective_region_id,
                        source.effective_region_name,
                        'REGION',
                        source.parent_country_id_source,
                        source.total_population_change, 
                        source.dead_population_change,
                        CASE 
                            WHEN source.total_population_change > 0 
                            THEN (source.dead_population_change * 100.0) / source.total_population_change
                            ELSE 0.0 
                        END
                    )
                """
                print(f"Batch {batch_id}: Executing MERGE SQL for {full_iceberg_table_name}...")
                spark.sql(merge_sql)
                print(f"Batch {batch_id}: MERGE SQL executed for {full_iceberg_table_name}.")
                
                # Always display current statistics after attempting processing for a non-empty batch
                display_current_statistics(spark, full_iceberg_table_name, batch_id)

            else:
                print(f"Batch {batch_id}: No population deltas calculated in this batch.")
                # Always display current statistics after attempting processing for a non-empty batch
                display_current_statistics(spark, full_iceberg_table_name, batch_id)

    except Exception as e:
        print(f"Batch {batch_id}: ERROR during processing: {e}")
        # Optionally, display stats even on error to see the last potentially good state or current state
        print(f"Batch {batch_id}: Displaying statistics after error...")
        display_current_statistics(spark, full_iceberg_table_name, batch_id)

    print(f"--- Finished Batch ID: {batch_id} ---")

def display_current_statistics(spark, table_name, batch_id):
    """Queries the Iceberg table and prints a summary of mortality statistics."""
    try:
        print(f"Batch {batch_id}: Displaying current mortality statistics from {table_name}...")
        # Example: Show top 10 regions by mortality rate
        stats_df = spark.sql(f"""
            SELECT 
                region_id, 
                region_name, 
                total_population, 
                dead_population, 
                mortality_rate_percent 
            FROM {table_name} 
            ORDER BY mortality_rate_percent DESC, total_population DESC
            LIMIT 20
        """)
        
        if not stats_df.head(1):
            print(f"Batch {batch_id}: No statistics found in {table_name} or table is empty.")
        else:
            print(f"Batch {batch_id}: Current Top Regions by Mortality Rate (from {table_name}):")
            stats_df.show(truncate=False)
            
    except Exception as e:
        print(f"Batch {batch_id}: Error displaying statistics from {table_name}: {e}")

def main_stream():
    spark = None
    full_iceberg_table_name = "" # Ensure it's defined in the outer scope for finally block if needed
    try:
        spark = get_spark_session()

        # --- Iceberg Table Setup ---
        db_name, table_name_only = (
            ICEBERG_TABLE_NAME.split(".", 1)
            if "." in ICEBERG_TABLE_NAME
            else (None, ICEBERG_TABLE_NAME)
        )

        if db_name:
            print(f"Creating/checking namespace '{db_name}' in catalog spark_catalog...")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{db_name}")
            full_iceberg_table_name = f"spark_catalog.{db_name}.{table_name_only}"
        else:
            db_name = "default" 
            print(f"Using 'default' namespace in catalog spark_catalog.")
            full_iceberg_table_name = f"spark_catalog.default.{table_name_only}"

        print(f"Target Iceberg table: {full_iceberg_table_name}")

        # Corrected DDL string construction
        table_ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_iceberg_table_name} (
            region_id STRING,
            region_name STRING,
            region_type STRING,
            parent_country_id STRING,
            total_population BIGINT,
            dead_population BIGINT,
            mortality_rate_percent DOUBLE
        ) USING iceberg
        """
        print(f"Executing DDL for Iceberg table {full_iceberg_table_name} (if not exists)...")
        spark.sql(table_ddl)
        print("Iceberg table setup complete.")
        # --- End Iceberg Table Setup ---
        
        print("Reading initial static regions data...")
        regions_df_static = (
            spark.read.format("mongodb")
            .option("spark.mongodb.database", MONGO_URI.split('/')[-1].split('?')[0])
            .option("spark.mongodb.collection", REGIONS_COLLECTION)
            .option("readPreference", "primary")
            .schema(REGION_SCHEMA)
            .load()
            .withColumnRenamed("_id", "region_doc_id")
            .withColumnRenamed("name", "region_name")
            .withColumnRenamed("type", "region_type")
            .persist()
        )
        if regions_df_static.rdd.isEmpty():
            print("WARNING: Initial regions data is empty. Streaming might not work as expected.")
        else:
            print(f"Initial regions data loaded and cached ({regions_df_static.count()} entries).")

        # Prepare the specific subset of regions data needed for merge (actual REGIONs)
        actual_regions_for_merge_info = regions_df_static.filter(col("region_type") == lit("REGION")) \
            .select("region_doc_id", "parentRegionId") \
            .persist()
        print(f"Cached actual regions for merge info: {actual_regions_for_merge_info.count()} entries.")

        print(f"Initializing stream for Users collection: {USERS_COLLECTION}")
        users_stream_df = (
            spark.readStream.format("mongodb")
            .option("spark.mongodb.database", MONGO_URI.split('/')[-1].split('?')[0])
            .option("spark.mongodb.collection", USERS_COLLECTION)
            .option("spark.mongodb.change.stream.publish.full.document.only", "true")
            .schema(USER_SCHEMA)
            .load()
            .withColumnRenamed("_id", "user_doc_id")
        )

        query = (users_stream_df.writeStream
                 .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, regions_df_static, full_iceberg_table_name, actual_regions_for_merge_info))
                 .option("checkpointLocation", CHECKPOINT_LOCATION)
                 .trigger(processingTime='30 seconds')
                 .start())
        
        print(f"Streaming query '{query.name}' started. Batch Interval: 30 seconds. Checkpoint: {CHECKPOINT_LOCATION}, Target Iceberg Table: {full_iceberg_table_name}")
        print("Awaiting termination... (Ctrl+C to stop)")
        query.awaitTermination()

    except Exception as e:
        print(f"An error occurred during streaming: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if spark:
            if 'regions_df_static' in locals() and regions_df_static is not None:
                print("Unpersisting static regions data...")
                regions_df_static.unpersist()
            if 'actual_regions_for_merge_info' in locals() and actual_regions_for_merge_info is not None:
                print("Unpersisting actual_regions_for_merge_info data...")
                actual_regions_for_merge_info.unpersist()
            print("Stopping Spark session...")
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    main_stream() 