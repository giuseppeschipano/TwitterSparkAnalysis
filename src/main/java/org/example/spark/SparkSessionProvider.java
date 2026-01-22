package org.example.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {

    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("TwitterSparkAnalysis")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") // JavaSerializer
                .config("spark.sql.execution.arrow.enabled", "false") // disabilito Arrow per evitare conflitti
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();
    }
}

