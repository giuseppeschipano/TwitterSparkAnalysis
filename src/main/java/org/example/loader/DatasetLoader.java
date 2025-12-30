package org.example.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class DatasetLoader {

    private final SparkSession spark;
    private final String dataFolder;

    public DatasetLoader(SparkSession spark, String dataFolder) {
        this.spark = spark;
        this.dataFolder = dataFolder;
    }

    public Dataset<Row> loadAllCSVs() {
        spark.sparkContext().setLogLevel("WARN");
        System.out.println("Caricamento dati da: " + dataFolder);

        try {
            // Passiamo direttamente la cartella. Spark leggerà tutti i CSV all'interno.
            return spark.read()
                    .option("header", "true")
                    .option("delimiter", ",")
                    .option("inferSchema", "false")
                    .option("ignoreCorruptFiles", "true")
                    .option("maxCharsPerColumn", "-1") // Previene errori su tweet molto lunghi
                    .csv(dataFolder);

        } catch (Exception e) {
            System.err.println("Errore durante la lettura: " + e.getMessage());
            return null;
        }
    }
}




