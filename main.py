from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    count,
    sum as _sum,
    broadcast,
    expr,
    coalesce,
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

MONGO_URI = "mongodb://localhost:27017/ZOV?replicaSet=rs0"
S3_BUCKET_WAREHOUSE = "s3a://zov-ta4ilka/iceberg_warehouse"

# Имена коллекций MongoDB
USERS_COLLECTION = "users"
REGIONS_COLLECTION = "regions"

# Имя таблицы Iceberg для результатов
ICEBERG_TABLE_NAME = "analytics.mortality_report_streaming"

# --- Схемы для чтения из MongoDB (опционально, но рекомендуется для надежности) ---
# Основано на ваших Java моделях
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
                [  # GeoJsonPolygon
                    StructField("type", StringType(), True),  # "Polygon"
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
        StructField(
            "populationCount", LongType(), True
        ),  # Это поле обновляется Java-приложением?
        StructField("importantPersonsCount", LongType(), True),
        StructField("underThreat", BooleanType(), True),
    ]
)


def get_spark_session():
    """Инициализирует и возвращает SparkSession с поддержкой Iceberg и S3."""
    
    current_mongo_uri = MONGO_URI # Capture global for clarity
    
    parsed_db_name_for_config = ""
    if current_mongo_uri:
        # Remove query parameters before splitting
        base_uri = current_mongo_uri.split('?')[0]
        uri_parts = base_uri.split('/')
        if len(uri_parts) > 3: # min: "mongodb:", "", "host", "db"
            if uri_parts[-1]: # Last part is db name, e.g., "mongodb://host/db"
                parsed_db_name_for_config = uri_parts[-1]
            elif len(uri_parts) > 4 and uri_parts[-2]: # Last part is empty (trailing slash), second to last is db, e.g., "mongodb://host/db/"
                 parsed_db_name_for_config = uri_parts[-2]

    if not parsed_db_name_for_config:
        print(f"Warning: Advanced parsing for database name from MONGO_URI ('{current_mongo_uri}') failed, trying simpler split.")
        base_uri_for_fallback = current_mongo_uri.split('?')[0] if current_mongo_uri else ""
        parsed_db_name_for_config = base_uri_for_fallback.split("/")[-1] if base_uri_for_fallback else ""

    print(f"Configuring Spark. MONGO_URI: '{current_mongo_uri}', Derived database name for config: '{parsed_db_name_for_config}'")
    
    spark_builder = (
        SparkSession.builder.appName("MongoDB_Iceberg_Analytics")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", S3_BUCKET_WAREHOUSE)
        .config("spark.mongodb.input.uri", current_mongo_uri)
        .config("spark.mongodb.input.database", parsed_db_name_for_config)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
        .config("spark.hadoop.fs.s3a.region", "ru-central1")
    )
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    return spark


def main():
    spark = get_spark_session()

    mongo_db_name_for_reader = ""
    if MONGO_URI:
        # Remove query parameters before splitting
        base_uri = MONGO_URI.split('?')[0]
        uri_parts = base_uri.split('/')
        if len(uri_parts) > 3: # e.g. ["mongodb:", "", "localhost:27017", "ZOV"]
            if uri_parts[-1]: # Last part is db name
                mongo_db_name_for_reader = uri_parts[-1]
            elif len(uri_parts) > 4 and uri_parts[-2]: # Last part is empty (trailing slash), second to last is db name
                 mongo_db_name_for_reader = uri_parts[-2]
    
    if not mongo_db_name_for_reader:
        print("ERROR: Could not determine MongoDB database name from MONGO_URI for Spark reader options.")
        base_uri_for_fallback = MONGO_URI.split('?')[0] if MONGO_URI else ""
        mongo_db_name_for_reader = base_uri_for_fallback.split("/")[-1] if base_uri_for_fallback else ""

    print(f"Using database '{mongo_db_name_for_reader}' for MongoDB read operations.")

    print("Загрузка данных из MongoDB...")
    users_df = (
        spark.read.format("mongodb")
        .option("database", mongo_db_name_for_reader)
        .option("collection", USERS_COLLECTION)
        .schema(USER_SCHEMA)
        .load()
        .withColumnRenamed("_id", "user_doc_id")
    )

    regions_df = (
        spark.read.format("mongodb")
        .option("database", mongo_db_name_for_reader)
        .option("collection", REGIONS_COLLECTION)
        .schema(REGION_SCHEMA)
        .load()
        .withColumnRenamed("_id", "region_doc_id")
        .withColumnRenamed("name", "region_name")
        .withColumnRenamed("type", "region_type")
    )

    print("Данные загружены.")
    users_df.printSchema()
    users_df.show(5, truncate=False)
    regions_df.printSchema()
    regions_df.show(5, truncate=False)

    # --- Новая логика агрегации ---

    # 1. Отфильтровываем REGION, CITY, DISTRICT для удобства
    actual_regions_df = regions_df.filter(col("region_type") == lit("REGION")).alias("actual_regions")
    cities_df = regions_df.filter(col("region_type") == lit("CITY")).alias("cities")
    districts_df = regions_df.filter(col("region_type") == lit("DISTRICT")).alias("districts")

    # 2. Отображение пользователей на их конечный REGION
    # Кейс 1: Пользователь привязан к districtId.
    # district -> city -> region
    users_via_district = users_df.filter(col("districtId").isNotNull()).alias("users_d")
    mapped_users_from_district = users_via_district.join(
        districts_df, col("users_d.districtId") == col("districts.region_doc_id"), "inner"
    ).join(
        cities_df, col("districts.parentRegionId") == col("cities.region_doc_id"), "inner"
    ).join(
        actual_regions_df, col("cities.parentRegionId") == col("actual_regions.region_doc_id"), "inner"
    ).select(
        col("users_d.user_doc_id"),
        col("users_d.active"),
        col("actual_regions.region_doc_id").alias("effective_region_id"),
        col("actual_regions.region_name").alias("effective_region_name")
    )

    # Кейс 2: Пользователь привязан к regionId, которое является CITY.
    # user.regionId (city) -> region
    users_via_city = users_df.filter(
        col("regionId").isNotNull() & col("districtId").isNull() # Нет districtId, есть regionId
    ).alias("users_c")
    mapped_users_from_city = users_via_city.join(
        cities_df, col("users_c.regionId") == col("cities.region_doc_id"), "inner" # Убеждаемся, что user.regionId - это город
    ).join(
        actual_regions_df, col("cities.parentRegionId") == col("actual_regions.region_doc_id"), "inner"
    ).select(
        col("users_c.user_doc_id"),
        col("users_c.active"),
        col("actual_regions.region_doc_id").alias("effective_region_id"),
        col("actual_regions.region_name").alias("effective_region_name")
    )

    # Кейс 3: Пользователь привязан к regionId, которое уже является REGION.
    users_via_region_direct = users_df.filter(
        col("regionId").isNotNull() & col("districtId").isNull() # Нет districtId, есть regionId
    ).alias("users_r_direct")
    mapped_users_direct_to_region = users_via_region_direct.join(
        actual_regions_df, col("users_r_direct.regionId") == col("actual_regions.region_doc_id"), "inner" # Убеждаемся, что user.regionId - это REGION
    ).select(
        col("users_r_direct.user_doc_id"),
        col("users_r_direct.active"),
        col("actual_regions.region_doc_id").alias("effective_region_id"),
        col("actual_regions.region_name").alias("effective_region_name")
    )
    
    # Объединяем все смапленные данные пользователей
    all_users_mapped_to_final_region = mapped_users_from_district.unionByName(
        mapped_users_from_city
    ).unionByName(
        mapped_users_direct_to_region
    ).distinct() # distinct на случай если пользователь может быть смаплен несколькими путями (маловероятно, но безопасно)

    print("Пользователи, смапленные на конечный REGION:")
    all_users_mapped_to_final_region.show(10, truncate=False)

    # 3. Считаем общее количество пользователей для каждого 'effective_region_id'
    total_population_per_region = all_users_mapped_to_final_region.groupBy(
        "effective_region_id", "effective_region_name" # Включаем имя для последующего отчета
    ).agg(count("user_doc_id").alias("total_population"))

    # 4. Считаем количество "умерших" пользователей для каждого 'effective_region_id'
    dead_population_per_region = (
        all_users_mapped_to_final_region.filter(col("active") == lit(False))
        .groupBy("effective_region_id", "effective_region_name")
        .agg(count("user_doc_id").alias("dead_population"))
    )

    print("Промежуточные агрегаты по REGION:")
    total_population_per_region.show(5)
    dead_population_per_region.show(5)

    # 5. Соединяем агрегаты для финального отчета
    report_df_base = actual_regions_df.select(
        col("actual_regions.region_doc_id").alias("region_id"),
        col("actual_regions.region_name"),
        col("actual_regions.parentRegionId").alias("parent_country_id")
    )

    report_with_total_pop = report_df_base.join(
        total_population_per_region,
        report_df_base["region_id"] == total_population_per_region["effective_region_id"],
        "left"
    ).select(
        report_df_base["*"], # Все колонки из report_df_base
        total_population_per_region["total_population"]
    )
    
    report_df = report_with_total_pop.join(
        dead_population_per_region,
        report_with_total_pop["region_id"] == dead_population_per_region["effective_region_id"],
        "left"
    ).select(
        report_with_total_pop["*"], # Все колонки из report_with_total_pop
        dead_population_per_region["dead_population"]
    ).select(
        col("region_id"),
        col("region_name"),
        lit("REGION").alias("region_type"),
        col("parent_country_id"),
        coalesce(col("total_population"), lit(0)).alias("total_population"),
        coalesce(col("dead_population"), lit(0)).alias("dead_population"),
    )

    # 6. Рассчитываем процент смертности и сортируем
    final_report_df = report_df.withColumn(
        "mortality_rate_percent",
        when(
            col("total_population") > 0,
            (col("dead_population") * 100.0 / col("total_population")),
        ).otherwise(0.0),
    ).orderBy(col("mortality_rate_percent").desc())

    print("Финальный отчет по REGION с наибольшей смертностью:")
    final_report_df.show(20, truncate=False)
    final_report_df.printSchema()

    # 8. Запись в Iceberg
    # Убедимся, что имя таблицы включает каталог (spark_catalog по умолчанию) и, возможно, базу данных
    # Например: spark_catalog.your_database.your_table
    # Если просто имя, то будет использована default база данных в каталоге.
    # Iceberg требует, чтобы база данных (неймспейс) существовала.
    # Можно создать ее через SQL: CREATE NAMESPACE IF NOT EXISTS spark_catalog.analytics;

    # Разделение имени таблицы на неймспейс и имя
    db_name, table_name_only = (
        ICEBERG_TABLE_NAME.split(".", 1)
        if "." in ICEBERG_TABLE_NAME
        else (None, ICEBERG_TABLE_NAME)
    )

    if db_name:
        print(f"Создание/проверка неймспейса '{db_name}' в каталоge spark_catalog...")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{db_name}")
        full_iceberg_table_name = f"spark_catalog.{db_name}.{table_name_only}"
    else:  # Если имя таблицы простое, используем default неймспейс
        full_iceberg_table_name = f"spark_catalog.default.{table_name_only}"  # или просто f"spark_catalog.{table_name_only}" если ваш каталог это позволяет

    print(f"Запись данных в Iceberg таблицу: {full_iceberg_table_name}")
    final_report_df.writeTo(
        full_iceberg_table_name
    ).createOrReplace()  # или .append() или .overwritePartitions()

    print(
        f"Данные успешно записаны в Iceberg таблицу {full_iceberg_table_name} в S3 хранилище {S3_BUCKET_WAREHOUSE}."
    )

    spark.stop()


if __name__ == "__main__":
    main()
