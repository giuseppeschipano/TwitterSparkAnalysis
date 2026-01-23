package org.example.preprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;
/**
 * Converte i CSV originali in Parquet, creando automaticamente la cartella di destinazione.

 * - CSV sono lenti da leggere e da parsare
 * - Spark deve fare parsing testuale ogni volta
 * - Parquet è colonnare, compresso e molto più veloce
 *
 * Operazione da eseguire una sola volta.
 */
public class CsvToParquetConverter {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Parquet Converter")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .getOrCreate();
        Dataset<Row> tempDf = null;
        for (int i = 1; i <= 31; i++) {
            String csvPath = "D:\\TwitterSparkAnalysis\\data\\tweet_USA_" + i + "_october.csv";
            File csvFile = new File(csvPath);
            if (!csvFile.exists()) {
                System.err.println("[WARN] File non trovato: " + csvPath + ", salto.");
                continue;
            }
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("delimiter", ",")
                    .option("multiLine", "true")
                    .option("inferSchema", "false")
                    .option("ignoreCorruptFiles", "true")
                    .csv(csvPath)
                    .sample(0.15); //solo 15% dati
            tempDf = (tempDf == null) ? df : tempDf.union(df);
        }
        if (tempDf != null) {
            String parquetPath = "D:\\TwitterSparkAnalysis\\parquet\\tweets_usa_2020";
            File parquetFolder = new File(parquetPath);
            parquetFolder.getParentFile().mkdirs();
            System.out.println("Salvataggio dataset Parquet in: " + parquetPath);
            tempDf.write()
                    .mode("overwrite")
                    .parquet(parquetPath);
            System.out.println("Conversione completata.");
        } else {
            System.err.println("[ERRORE] Nessun CSV caricato, controlla la cartella data.");
        }
        spark.stop();
    }
}

