package org.example.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {
    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("TwitterSparkAnalysis")
                .master("local[*]")
                // Questo comando dice a Spark: "Non usare le funzioni di Windows per leggere i file!"
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                .getOrCreate();
    }
}