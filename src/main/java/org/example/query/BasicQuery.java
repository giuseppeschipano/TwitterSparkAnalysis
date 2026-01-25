package org.example.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class BasicQuery {

    // --- Metodo principale per eseguire tutte le query ---
    public static void runAll(Dataset<Row> df) {
        System.out.println("\n--- ESECUZIONE QUERIES ---");

        numeroTotaleTweet(df);
        distribuzioneTweetPerStato1(df);
        andamentoGiornalieroTweet(df);
        topHashtag(df);
        topHashtagPerStato(df);
        hashtagIntenzioniVoto(df);
        utentiUniciIntenzioniVoto(df);
        evoluzioneTweetPerCandidato(df);
        calcolaVincitorePerStato(df);
    }

    public static void numeroTotaleTweet(Dataset<Row> df) {
        System.out.println("\n[TOTALE TWEET]");
        System.out.println("Numero di tweet considerati: " + df.count());
    }

    public static Dataset<Row> distribuzioneTweetPerStato1(Dataset<Row> df) {
        Dataset<Row> filtered = df.filter(
                col("location").isNotNull()
                        .and(not(col("location").equalTo("NaN")))
                        .and(length(trim(col("location"))).gt(0))
        );
        Dataset<Row> normalized = filtered.withColumn("normalized_state",
                when(col("location").rlike("(?i)AL|Alabama"), "Alabama")
                        .when(col("location").rlike("(?i)AK|Alaska"), "Alaska")
                        .when(col("location").rlike("(?i)AZ|Arizona"), "Arizona")
                        .when(col("location").rlike("(?i)AR|Arkansas"), "Arkansas")
                        .when(col("location").rlike("(?i)CA|California"), "California")
                        .when(col("location").rlike("(?i)CO|Colorado"), "Colorado")
                        .when(col("location").rlike("(?i)CT|Connecticut"), "Connecticut")
                        .when(col("location").rlike("(?i)DE|Delaware"), "Delaware")
                        .when(col("location").rlike("(?i)FL|Florida"), "Florida")
                        .when(col("location").rlike("(?i)GA|Georgia"), "Georgia")
                        .when(col("location").rlike("(?i)HI|Hawaii"), "Hawaii")
                        .when(col("location").rlike("(?i)ID|Idaho"), "Idaho")
                        .when(col("location").rlike("(?i)IL|Illinois"), "Illinois")
                        .when(col("location").rlike("(?i)IN|Indiana"), "Indiana")
                        .when(col("location").rlike("(?i)IA|Iowa"), "Iowa")
                        .when(col("location").rlike("(?i)KS|Kansas"), "Kansas")
                        .when(col("location").rlike("(?i)KY|Kentucky"), "Kentucky")
                        .when(col("location").rlike("(?i)LA|Louisiana"), "Louisiana")
                        .when(col("location").rlike("(?i)ME|Maine"), "Maine")
                        .when(col("location").rlike("(?i)MD|Maryland"), "Maryland")
                        .when(col("location").rlike("(?i)MA|Massachusetts"), "Massachusetts")
                        .when(col("location").rlike("(?i)MI|Michigan"), "Michigan")
                        .when(col("location").rlike("(?i)MN|Minnesota"), "Minnesota")
                        .when(col("location").rlike("(?i)MS|Mississippi"), "Mississippi")
                        .when(col("location").rlike("(?i)MO|Missouri"), "Missouri")
                        .when(col("location").rlike("(?i)MT|Montana"), "Montana")
                        .when(col("location").rlike("(?i)NE|Nebraska"), "Nebraska")
                        .when(col("location").rlike("(?i)NV|Nevada"), "Nevada")
                        .when(col("location").rlike("(?i)NH|New Hampshire"), "New Hampshire")
                        .when(col("location").rlike("(?i)NJ|New Jersey"), "New Jersey")
                        .when(col("location").rlike("(?i)NM|New Mexico"), "New Mexico")
                        .when(col("location").rlike("(?i)NY|New York"), "New York")
                        .when(col("location").rlike("(?i)NC|North Carolina"), "North Carolina")
                        .when(col("location").rlike("(?i)ND|North Dakota"), "North Dakota")
                        .when(col("location").rlike("(?i)OH|Ohio"), "Ohio")
                        .when(col("location").rlike("(?i)OK|Oklahoma"), "Oklahoma")
                        .when(col("location").rlike("(?i)OR|Oregon"), "Oregon")
                        .when(col("location").rlike("(?i)PA|Pennsylvania"), "Pennsylvania")
                        .when(col("location").rlike("(?i)RI|Rhode Island"), "Rhode Island")
                        .when(col("location").rlike("(?i)SC|South Carolina"), "South Carolina")
                        .when(col("location").rlike("(?i)SD|South Dakota"), "South Dakota")
                        .when(col("location").rlike("(?i)TN|Tennessee"), "Tennessee")
                        .when(col("location").rlike("(?i)TX|Texas"), "Texas")
                        .when(col("location").rlike("(?i)UT|Utah"), "Utah")
                        .when(col("location").rlike("(?i)VT|Vermont"), "Vermont")
                        .when(col("location").rlike("(?i)VA|Virginia"), "Virginia")
                        .when(col("location").rlike("(?i)WA|Washington"), "Washington")
                        .when(col("location").rlike("(?i)WV|West Virginia"), "West Virginia")
                        .when(col("location").rlike("(?i)WI|Wisconsin"), "Wisconsin")
                        .when(col("location").rlike("(?i)WY|Wyoming"), "Wyoming")
                        .otherwise("Other")
        );
        Dataset<Row> result = normalized
                .filter(col("normalized_state").notEqual("Other"))
                .groupBy("normalized_state")
                .count()
                .orderBy(desc("count"));
        return result;
    }


    public static Dataset<Row> andamentoGiornalieroTweet(Dataset<Row> df) {
        return df
                .filter(col("created_at").isNotNull()
                        .and(not(col("created_at").equalTo("NaN"))))
                .withColumn(
                        "timestamp",
                        to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss")
                )
                .filter(col("timestamp").isNotNull())
                .withColumn("date", to_date(col("timestamp")))
                .groupBy("date")
                .count()
                .orderBy("date");
    }


    public static Dataset<Row> topHashtag(Dataset<Row> dfTweets) {
        List<String> blacklist = Arrays.asList("etc", "nan", "na", "null", "none");
        Dataset<Row> top100 = dfTweets
                .withColumn("hashtag_clean", regexp_replace(col("hashtags"), "[\\[\\]\"]", ""))
                .withColumn("tag", explode(split(lower(col("hashtag_clean")), ",")))
                .withColumn("tag", trim(col("tag")))
                .filter(col("tag").rlike(".*[a-z].*"))
                .filter(not(col("tag").isin(blacklist.toArray())))
                .filter(length(col("tag")).gt(2))
                .groupBy("tag").count()
                .orderBy(desc("count"))
                .limit(10);
        return top100;
    }

    public static Dataset<Row> topHashtagPerStato(Dataset<Row> dfTweets) {
        Dataset<Row> dfNormalized = dfTweets.withColumn("state_clean",
                when(col("location").rlike("(?i)AL|Alabama"), "Alabama")
                        .when(col("location").rlike("(?i)AK|Alaska"), "Alaska")
                        .when(col("location").rlike("(?i)AZ|Arizona"), "Arizona")
                        .when(col("location").rlike("(?i)AR|Arkansas"), "Arkansas")
                        .when(col("location").rlike("(?i)CA|California"), "California")
                        .when(col("location").rlike("(?i)CO|Colorado"), "Colorado")
                        .when(col("location").rlike("(?i)CT|Connecticut"), "Connecticut")
                        .when(col("location").rlike("(?i)DE|Delaware"), "Delaware")
                        .when(col("location").rlike("(?i)FL|Florida"), "Florida")
                        .when(col("location").rlike("(?i)GA|Georgia"), "Georgia")
                        .when(col("location").rlike("(?i)HI|Hawaii"), "Hawaii")
                        .when(col("location").rlike("(?i)ID|Idaho"), "Idaho")
                        .when(col("location").rlike("(?i)IL|Illinois"), "Illinois")
                        .when(col("location").rlike("(?i)IN|Indiana"), "Indiana")
                        .when(col("location").rlike("(?i)IA|Iowa"), "Iowa")
                        .when(col("location").rlike("(?i)KS|Kansas"), "Kansas")
                        .when(col("location").rlike("(?i)KY|Kentucky"), "Kentucky")
                        .when(col("location").rlike("(?i)LA|Louisiana"), "Louisiana")
                        .when(col("location").rlike("(?i)ME|Maine"), "Maine")
                        .when(col("location").rlike("(?i)MD|Maryland"), "Maryland")
                        .when(col("location").rlike("(?i)MA|Massachusetts"), "Massachusetts")
                        .when(col("location").rlike("(?i)MI|Michigan"), "Michigan")
                        .when(col("location").rlike("(?i)MN|Minnesota"), "Minnesota")
                        .when(col("location").rlike("(?i)MS|Mississippi"), "Mississippi")
                        .when(col("location").rlike("(?i)MO|Missouri"), "Missouri")
                        .when(col("location").rlike("(?i)MT|Montana"), "Montana")
                        .when(col("location").rlike("(?i)NE|Nebraska"), "Nebraska")
                        .when(col("location").rlike("(?i)NV|Nevada"), "Nevada")
                        .when(col("location").rlike("(?i)NH|New Hampshire"), "New Hampshire")
                        .when(col("location").rlike("(?i)NJ|New Jersey"), "New Jersey")
                        .when(col("location").rlike("(?i)NM|New Mexico"), "New Mexico")
                        .when(col("location").rlike("(?i)NY|New York"), "New York")
                        .when(col("location").rlike("(?i)NC|North Carolina"), "North Carolina")
                        .when(col("location").rlike("(?i)ND|North Dakota"), "North Dakota")
                        .when(col("location").rlike("(?i)OH|Ohio"), "Ohio")
                        .when(col("location").rlike("(?i)OK|Oklahoma"), "Oklahoma")
                        .when(col("location").rlike("(?i)OR|Oregon"), "Oregon")
                        .when(col("location").rlike("(?i)PA|Pennsylvania"), "Pennsylvania")
                        .when(col("location").rlike("(?i)RI|Rhode Island"), "Rhode Island")
                        .when(col("location").rlike("(?i)SC|South Carolina"), "South Carolina")
                        .when(col("location").rlike("(?i)SD|South Dakota"), "South Dakota")
                        .when(col("location").rlike("(?i)TN|Tennessee"), "Tennessee")
                        .when(col("location").rlike("(?i)TX|Texas"), "Texas")
                        .when(col("location").rlike("(?i)UT|Utah"), "Utah")
                        .when(col("location").rlike("(?i)VT|Vermont"), "Vermont")
                        .when(col("location").rlike("(?i)VA|Virginia"), "Virginia")
                        .when(col("location").rlike("(?i)WA|Washington"), "Washington")
                        .when(col("location").rlike("(?i)WV|West Virginia"), "West Virginia")
                        .when(col("location").rlike("(?i)WI|Wisconsin"), "Wisconsin")
                        .when(col("location").rlike("(?i)WY|Wyoming"), "Wyoming")
                        .otherwise("Other")
        ).filter(col("state_clean").notEqual("Other").and(col("state_clean").notEqual("Unknown")));
        Dataset<Row> tagsExploded = dfNormalized
                .filter(col("hashtags").isNotNull().and(not(col("hashtags").equalTo("[]"))))
                .withColumn("tag", explode(split(lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")), ",")))
                .withColumn("tag", trim(col("tag")))
                .filter(length(col("tag")).gt(2))
                .filter(col("tag").rlike(".*[a-z].*"));
        Dataset<Row> topHashtagStato = tagsExploded
                .groupBy("state_clean", "tag")
                .count()
                .withColumn("rank", row_number()
                        .over(Window.partitionBy("state_clean").orderBy(col("count").desc())))
                .filter(col("rank").leq(5))
                .orderBy("state_clean", "rank");

        return topHashtagStato;
    }


    // Tweet totali per intenzioni di voto (utenti coerenti), misura il volume di tweet politicamente schierati
    public static Dataset<Row> hashtagIntenzioniVoto(Dataset<Row> df) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");

        //Flag dei tweet per candidato
        Dataset<Row> flaggedTweets = df
                .filter(col("hashtags").isNotNull().and(not(col("hashtags").equalTo("[]"))))
                .withColumn("hashtags_clean", lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")))
                .withColumn("isBiden",
                        array_contains(split(col("hashtags_clean"), ","), bidenTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(2))))
                .withColumn("isTrump",
                        array_contains(split(col("hashtags_clean"), ","), trumpTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(2))))
                .filter(col("isBiden").notEqual(col("isTrump"))); // elimina tweet misti

        // Aggregazione per candidato
        Dataset<Row> result = flaggedTweets
                .withColumn("voto", when(col("isBiden"), "Biden").otherwise("Trump"))
                .groupBy("voto")
                .count()
                .withColumnRenamed("count", "tweet_count")
                .orderBy(desc("tweet_count"));
        return result;
    }



    // Utenti unici per intenzioni di voto
    public static Dataset<Row> utentiUniciIntenzioniVoto(Dataset<Row> df) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");

        // Flag dei tweet per candidato
        Dataset<Row> flaggedTweets = df
                .filter(col("hashtags").isNotNull().and(not(col("hashtags").equalTo("[]"))))
                .withColumn("hashtags_clean", lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")))
                .withColumn("isBiden",
                        array_contains(split(col("hashtags_clean"), ","), bidenTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(2))))
                .withColumn("isTrump",
                        array_contains(split(col("hashtags_clean"), ","), trumpTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(2))))
                .filter(col("isBiden").notEqual(col("isTrump"))); // elimina tweet misti

        // Determiniamo utenti coerenti
        Dataset<Row> utentiCoerenti = flaggedTweets
                .groupBy("user_id_str")
                .agg(
                        sum(when(col("isBiden"), 1).otherwise(0)).as("b_count"),
                        sum(when(col("isTrump"), 1).otherwise(0)).as("t_count")
                )
                .filter(
                        (col("b_count").gt(0).and(col("t_count").equalTo(0)))
                                .or(col("t_count").gt(0).and(col("b_count").equalTo(0)))
                );

        // Conteggio utenti unici coerenti
        Dataset<Row> result = utentiCoerenti
                .withColumn("elettore", when(col("b_count").gt(0), "Biden").otherwise("Trump"))
                .groupBy("elettore")
                .count()
                .withColumnRenamed("count", "utenti_unici")
                .orderBy(desc("utenti_unici"));

        return result;
    }



    //volume
    public static Dataset<Row> evoluzioneTweetPerCandidato(Dataset<Row> dfTweets) {

        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");

        // Flag dei tweet per candidato
        Dataset<Row> flaggedTweets = dfTweets
                .filter(col("hashtags").isNotNull().and(not(col("hashtags").equalTo("[]"))))
                .withColumn("hashtags_clean", lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")))
                .withColumn("isBiden",
                        array_contains(split(col("hashtags_clean"), ","), bidenTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(2))))
                .withColumn("isTrump",
                        array_contains(split(col("hashtags_clean"), ","), trumpTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(2))))
                .filter(col("isBiden").notEqual(col("isTrump"))); // elimina tweet misti

        // Determiniamo utenti coerenti
        Dataset<Row> utentiCoerenti = flaggedTweets
                .groupBy("user_id_str")
                .agg(
                        sum(when(col("isBiden"), 1).otherwise(0)).as("b_count"),
                        sum(when(col("isTrump"), 1).otherwise(0)).as("t_count")
                )
                .filter(
                        (col("b_count").gt(0).and(col("t_count").equalTo(0)))
                                .or(col("t_count").gt(0).and(col("b_count").equalTo(0)))
                );

        // Recuperiamo solo i tweet degli utenti coerenti e aggiungiamo voto
        Dataset<Row> pureTweets = flaggedTweets
                .join(utentiCoerenti.select("user_id_str"), "user_id_str")
                .withColumn("voto", when(col("isBiden"), "Biden").otherwise("Trump"))
                .withColumn("date", to_date(col("created_at"), "yyyy-MM-dd"))
                .filter(col("date").isNotNull());

        //  Aggregazione finale per giorno
        Dataset<Row> evoluzione = pureTweets
                .groupBy("date")
                .agg(
                        sum(when(col("voto").equalTo("Biden"), 1).otherwise(0)).as("Biden"),
                        sum(when(col("voto").equalTo("Trump"), 1).otherwise(0)).as("Trump")
                )
                .orderBy("date");
        return evoluzione;
    }


    public static Dataset<Row> calcolaVincitorePerStato(Dataset<Row> dfTweets) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");

        // Normalizzazione dello stato
        Dataset<Row> dfNormalized = dfTweets
                .withColumn("state_clean",
                        when(col("location").rlike("(?i)AL|Alabama"), "Alabama")
                                .when(col("location").rlike("(?i)AK|Alaska"), "Alaska")
                                .when(col("location").rlike("(?i)AZ|Arizona"), "Arizona")
                                .when(col("location").rlike("(?i)AR|Arkansas"), "Arkansas")
                                .when(col("location").rlike("(?i)CA|California"), "California")
                                .when(col("location").rlike("(?i)CO|Colorado"), "Colorado")
                                .when(col("location").rlike("(?i)CT|Connecticut"), "Connecticut")
                                .when(col("location").rlike("(?i)DE|Delaware"), "Delaware")
                                .when(col("location").rlike("(?i)FL|Florida"), "Florida")
                                .when(col("location").rlike("(?i)GA|Georgia"), "Georgia")
                                .when(col("location").rlike("(?i)HI|Hawaii"), "Hawaii")
                                .when(col("location").rlike("(?i)ID|Idaho"), "Idaho")
                                .when(col("location").rlike("(?i)IL|Illinois"), "Illinois")
                                .when(col("location").rlike("(?i)IN|Indiana"), "Indiana")
                                .when(col("location").rlike("(?i)IA|Iowa"), "Iowa")
                                .when(col("location").rlike("(?i)KS|Kansas"), "Kansas")
                                .when(col("location").rlike("(?i)KY|Kentucky"), "Kentucky")
                                .when(col("location").rlike("(?i)LA|Louisiana"), "Louisiana")
                                .when(col("location").rlike("(?i)ME|Maine"), "Maine")
                                .when(col("location").rlike("(?i)MD|Maryland"), "Maryland")
                                .when(col("location").rlike("(?i)MA|Massachusetts"), "Massachusetts")
                                .when(col("location").rlike("(?i)MI|Michigan"), "Michigan")
                                .when(col("location").rlike("(?i)MN|Minnesota"), "Minnesota")
                                .when(col("location").rlike("(?i)MS|Mississippi"), "Mississippi")
                                .when(col("location").rlike("(?i)MO|Missouri"), "Missouri")
                                .when(col("location").rlike("(?i)MT|Montana"), "Montana")
                                .when(col("location").rlike("(?i)NE|Nebraska"), "Nebraska")
                                .when(col("location").rlike("(?i)NV|Nevada"), "Nevada")
                                .when(col("location").rlike("(?i)NH|New Hampshire"), "New Hampshire")
                                .when(col("location").rlike("(?i)NJ|New Jersey"), "New Jersey")
                                .when(col("location").rlike("(?i)NM|New Mexico"), "New Mexico")
                                .when(col("location").rlike("(?i)NY|New York"), "New York")
                                .when(col("location").rlike("(?i)NC|North Carolina"), "North Carolina")
                                .when(col("location").rlike("(?i)ND|North Dakota"), "North Dakota")
                                .when(col("location").rlike("(?i)OH|Ohio"), "Ohio")
                                .when(col("location").rlike("(?i)OK|Oklahoma"), "Oklahoma")
                                .when(col("location").rlike("(?i)OR|Oregon"), "Oregon")
                                .when(col("location").rlike("(?i)PA|Pennsylvania"), "Pennsylvania")
                                .when(col("location").rlike("(?i)RI|Rhode Island"), "Rhode Island")
                                .when(col("location").rlike("(?i)SC|South Carolina"), "South Carolina")
                                .when(col("location").rlike("(?i)SD|South Dakota"), "South Dakota")
                                .when(col("location").rlike("(?i)TN|Tennessee"), "Tennessee")
                                .when(col("location").rlike("(?i)TX|Texas"), "Texas")
                                .when(col("location").rlike("(?i)UT|Utah"), "Utah")
                                .when(col("location").rlike("(?i)VT|Vermont"), "Vermont")
                                .when(col("location").rlike("(?i)VA|Virginia"), "Virginia")
                                .when(col("location").rlike("(?i)WA|Washington"), "Washington")
                                .when(col("location").rlike("(?i)WV|West Virginia"), "West Virginia")
                                .when(col("location").rlike("(?i)WI|Wisconsin"), "Wisconsin")
                                .when(col("location").rlike("(?i)WY|Wyoming"), "Wyoming")
                                .otherwise("Other"))
                .filter(col("state_clean").notEqual("Other"))
                .filter(col("user_id_str").isNotNull());

        // Flag dei tweet per candidato (senza explode)
        Dataset<Row> flaggedTweets = dfNormalized
                .withColumn("hashtags_clean", lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")))
                .withColumn("isBiden",
                        array_contains(split(col("hashtags_clean"), ","), bidenTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), bidenTags.get(2))))
                .withColumn("isTrump",
                        array_contains(split(col("hashtags_clean"), ","), trumpTags.get(0))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(1)))
                                .or(array_contains(split(col("hashtags_clean"), ","), trumpTags.get(2))))
                .filter(col("isBiden").notEqual(col("isTrump"))); // elimina tweet misti

        // Identificazione utenti coerenti (1 utente = 1 voto)
        Dataset<Row> utentiCoerenti = flaggedTweets
                .groupBy("user_id_str", "state_clean")
                .agg(
                        sum(when(col("isBiden"), 1).otherwise(0)).as("biden_count"),
                        sum(when(col("isTrump"), 1).otherwise(0)).as("trump_count")
                )
                .filter(
                        (col("biden_count").gt(0).and(col("trump_count").equalTo(0)))
                                .or(col("trump_count").gt(0).and(col("biden_count").equalTo(0)))
                )
                .withColumn("voto", when(col("biden_count").gt(0), "Biden").otherwise("Trump"));

        // Conteggio per stato
        Dataset<Row> stateSummary = utentiCoerenti
                .groupBy("state_clean")
                .agg(
                        sum(when(col("voto").equalTo("Biden"), 1).otherwise(0)).as("Biden"),
                        sum(when(col("voto").equalTo("Trump"), 1).otherwise(0)).as("Trump")
                )
                .orderBy("state_clean");

        // Calcolo vincitore
        Dataset<Row> finalResults = stateSummary
                .withColumn("Vincitore", when(col("Biden").gt(col("Trump")), "Biden").otherwise("Trump"))
                .withColumn("Differenza", abs(col("Biden").minus(col("Trump"))));

        return finalResults;
    }
}


