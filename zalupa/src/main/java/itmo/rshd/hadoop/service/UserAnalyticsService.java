package itmo.rshd.hadoop.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import itmo.rshd.hadoop.model.Region;

@Service
public class UserAnalyticsService implements CommandLineRunner {

    private final SparkSession sparkSession;

    @Value("${mongodb.database}")
    private String mongoDbDatabase;

    @Value("${spark.iceberg.catalog}")
    private String icebergCatalog;

    public UserAnalyticsService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Starting User Analytics Service...");

        // Load users data from MongoDB
        Dataset<Row> usersDF = sparkSession.read()
                .format("mongodb")
                .option("database", mongoDbDatabase)
                .option("collection", "users")
                .load();

        // Load regions data from MongoDB
        Dataset<Row> regionsDF = sparkSession.read()
                .format("mongodb")
                .option("database", mongoDbDatabase)
                .option("collection", "regions")
                .load();

        // Filter for deceased users (active = false)
        Dataset<Row> deceasedUsersDF = usersDF.filter(col("active").equalTo(false));

        // Filter for regions of type REGION
        Dataset<Row> actualRegionsDF = regionsDF.filter(col("type").equalTo(Region.RegionType.REGION.toString()));

        // Join deceased users with their respective regions
        // Assuming users have a 'regionId' field that maps to Region's '_id' (or 'id' if mapped)
        Dataset<Row> deceasedUsersInRegionsDF = deceasedUsersDF
                .join(actualRegionsDF, deceasedUsersDF.col("regionId").equalTo(actualRegionsDF.col("_id")), "inner");

        // Group by region name and count deceased users
        Dataset<Row> deceasedByRegionStatsDF = deceasedUsersInRegionsDF
                .groupBy(actualRegionsDF.col("name").alias("region_name"))
                .agg(count("*").alias("deceased_count"))
                .orderBy(desc("deceased_count"));

        System.out.println("Deceased users by region statistics:");
        deceasedByRegionStatsDF.show(false); // Show all rows without truncation

        if (!deceasedByRegionStatsDF.isEmpty()) {
            Row topRegion = deceasedByRegionStatsDF.first();
            System.out.println("Region with the most deceased individuals: " + topRegion.getString(0) + " with " + topRegion.getLong(1) + " deceased.");
        }

        // Define the Iceberg table name
        String tableName = String.format("%s.analytics.deceased_by_region_stats", icebergCatalog);

        // Create database if not exists (Spark 3.x syntax for Iceberg)
        sparkSession.sql(String.format("CREATE DATABASE IF NOT EXISTS %s.analytics", icebergCatalog));

        // Write the aggregated statistics to an Iceberg table
        System.out.println("Writing statistics to Iceberg table: " + tableName);
        deceasedByRegionStatsDF.write()
                .format("iceberg")
                .mode("overwrite") // Overwrite existing data or use "append"
                .save(tableName);

        System.out.println("Successfully wrote statistics to Iceberg table: " + tableName);

        // Example: Read data back from Iceberg to verify
        System.out.println("Reading data back from Iceberg table: " + tableName);
        Dataset<Row> icebergData = sparkSession.read()
                .format("iceberg")
                .load(tableName);
        icebergData.show();

        System.out.println("User Analytics Service finished.");
        // The application will exit after this CommandLineRunner finishes due to Spring Boot behavior.
    }
} 