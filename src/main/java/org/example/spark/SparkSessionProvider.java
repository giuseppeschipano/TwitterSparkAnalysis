package org.example.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionProvider {

    public static SparkSession createSession() {
        return SparkSession.builder()
                .appName("TwitterSparkAnalysis")
                .master("local[*]") // Utilizzo tutti i core della CPU disponibili sulla macchina locale
                .config("spark.driver.memory", "4g") // Allocazione 4GB di RAM per il processo Driver
                .config("spark.executor.memory", "4g") // Allocazione 4GB di RAM per gli Executor (elaboratori dati)
                .config("spark.sql.shuffle.partitions", "8") // Riduzione il numero di partizioni per ottimizzare le performance su dataset medi
                .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") // Protocollo Java per la serializzazione
                .config("spark.sql.execution.arrow.enabled", "false") // Disabilito Apache Arrow per evitare conflitti
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Utilizzo del vecchio sistema di parsing delle date per evitare errori di formato
                .getOrCreate();
    }
}

