package org.example.loader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Carica dataset già preprocessati in Parquet.
 * - La GUI deve essere reattiva
 * - CSV pesanti preprocessati offline
 * - Parquet è veloce e adatto alle query interattive
 */
public class DatasetLoader {

    private final SparkSession spark;
    private final String parquetPath;

    public DatasetLoader(SparkSession spark, String parquetPath) {
        this.spark = spark;
        this.parquetPath = parquetPath;
    }

    public Dataset<Row> loadDataset() {
        File parquetDir = new File(parquetPath);
        if (!parquetDir.exists()) {
            System.err.println("[ERRORE] Path Parquet non trovato: " + parquetPath);
            return null;
        }
        System.out.println("Caricamento dataset Parquet da: " + parquetPath);
        Dataset<Row> df = spark.read()
                .parquet(parquetPath)
                .cache();  // Mantengo in memoria per velocità
        df.count();
        return df;
    }
}


/*
Ho separato la fase di caricamento dati dalla GUI.
DatasetLoader carica solo dati già ottimizzati in Parquet,
rendendo l’interazione veloce e stabile.
 */









