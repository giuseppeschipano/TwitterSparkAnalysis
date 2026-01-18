package org.example.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {
/*
    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("TwitterSparkAnalysis")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .getOrCreate();
    }
*/
    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("TwitterSparkAnalysis")
                .master("local[2]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .getOrCreate();
    }

}