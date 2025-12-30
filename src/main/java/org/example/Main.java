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
        System.setProperty("spark.hadoop.fs.configuration", "org.apache.hadoop.conf.Configuration");

        // 2. Patch per disabilitare il caricamento nativo
        applyHadoopPatch();

        System.out.println("Avvio Spark Session...");
        SparkSession spark = null;
        try {
            spark = SparkSessionProvider.createSession();

            String dataPath = "D:\\TwitterSparkAnalysis\\data";
            System.out.println("Cartella dati: " + dataPath);

            // Trasformiamo il percorso in formato URI per evitare problemi di backslash
            String dataPathUri = new File(dataPath).toURI().toString();

            System.out.println("Caricamento dati da: " + dataPathUri);
            DatasetLoader loader = new DatasetLoader(spark, dataPathUri);
            Dataset<Row> df = loader.loadAllCSVs();

            if (df != null) {
                System.out.println("Dataset collegato con successo!");
                df.show(5);
            }
        } catch (Exception e) {
            System.err.println("Errore critico: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (spark != null) spark.stop();
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

            System.out.println("Patch applicata. Codice nativo bypassato.");
        } catch (Exception e) {
            System.out.println("Patch non applicata: " + e.getMessage());
        }
    }
}