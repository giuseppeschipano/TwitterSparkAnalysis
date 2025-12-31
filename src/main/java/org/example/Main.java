package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.loader.DatasetLoader;
import org.example.spark.SparkSessionProvider;
import java.io.File;
import java.lang.reflect.Field;

public class Main {
    public static void main(String[] args) {
        // 1. Configurazione Windows & Hadoop
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        System.setProperty("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");

        // 2. Patch per bypassare il codice nativo di Windows
        applyHadoopPatch();

        System.out.println("Avvio Twitter Spark Analysis");
        SparkSession spark = null;

        try {
            spark = SparkSessionProvider.createSession();
            spark.sparkContext().setLogLevel("ERROR");

            String dataPath = "D:\\TwitterSparkAnalysis\\data";
            String dataPathUri = new File(dataPath).toURI().toString();

            System.out.println("Cerco i file in: " + dataPath);

            DatasetLoader loader = new DatasetLoader(spark, dataPathUri);
            Dataset<Row> df = loader.loadAllCSVs();

            if (df == null) {
                System.err.println("Errore: df è nullo!");
            } else {
                System.out.println("Dataset non nullo, righe caricabili...");

                System.out.println("\n STRUTTURA DELLE COLONNE ");
                df.printSchema();
            }

        } catch (Exception e) {
            System.err.println("ERRORE DURANTE L'ESECUZIONE: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (spark != null) {
                System.out.println("Chiusura Spark Session");
                spark.stop();
            }
        }
    }

    private static void applyHadoopPatch() {
        try {
            // Disabilita NativeCodeLoader
            Field ncl = org.apache.hadoop.util.NativeCodeLoader.class.getDeclaredField("nativeCodeLoaded");
            ncl.setAccessible(true);
            ncl.setBoolean(null, false);

            // Inietta il flag nativeLoaded = false in NativeIO
            Field nl = org.apache.hadoop.io.nativeio.NativeIO.class.getDeclaredField("nativeLoaded");
            nl.setAccessible(true);
            nl.setBoolean(null, false);

        } catch (Exception e) {
        }
    }
}