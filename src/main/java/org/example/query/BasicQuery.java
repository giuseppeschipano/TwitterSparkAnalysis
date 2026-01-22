package org.example.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.spark.sql.functions.*;

public class BasicQuery {

    // --- Metodo principale per eseguire tutte le query ---
    public static void runAll(Dataset<Row> df) {
        System.out.println("\n--- ESECUZIONE QUERIES ---");

        registerNormalizeStateUDF(df);
        numeroTotaleTweet(df);
        distribuzioneTweetPerStato1(df);
        distribuzioneTweetPerStato2(df);
        andamentoGiornalieroTweet(df);
        proporzioneContenuti(df);
        topHashtag(df);
        topHashtagPerStato(df);
        hashtagIntenzioniVoto(df);
        utentiUniciIntenzioniVoto(df);
        evoluzioneTweetPerCandidato(df);
        calcolaVincitorePerStato(df);
    }

    // --- UDF per normalizzare lo stato ---
    private static void registerNormalizeStateUDF(Dataset<Row> df) {
        Map<String, String> stateMap = new HashMap<>();
        stateMap.put("AL", "Alabama"); stateMap.put("AK", "Alaska"); stateMap.put("AZ", "Arizona"); stateMap.put("AR", "Arkansas");
        stateMap.put("CA", "California"); stateMap.put("CO", "Colorado"); stateMap.put("CT", "Connecticut"); stateMap.put("DE", "Delaware");
        stateMap.put("FL", "Florida"); stateMap.put("GA", "Georgia"); stateMap.put("HI", "Hawaii"); stateMap.put("ID", "Idaho");
        stateMap.put("IL", "Illinois"); stateMap.put("IN", "Indiana"); stateMap.put("IA", "Iowa"); stateMap.put("KS", "Kansas");
        stateMap.put("KY", "Kentucky"); stateMap.put("LA", "Louisiana"); stateMap.put("ME", "Maine"); stateMap.put("MD", "Maryland");
        stateMap.put("MA", "Massachusetts"); stateMap.put("MI", "Michigan"); stateMap.put("MN", "Minnesota"); stateMap.put("MS", "Mississippi");
        stateMap.put("MO", "Missouri"); stateMap.put("MT", "Montana"); stateMap.put("NE", "Nebraska"); stateMap.put("NV", "Nevada");
        stateMap.put("NH", "New Hampshire"); stateMap.put("NJ", "New Jersey"); stateMap.put("NM", "New Mexico"); stateMap.put("NY", "New York");
        stateMap.put("NC", "North Carolina"); stateMap.put("ND", "North Dakota"); stateMap.put("OH", "Ohio"); stateMap.put("OK", "Oklahoma");
        stateMap.put("OR", "Oregon"); stateMap.put("PA", "Pennsylvania"); stateMap.put("RI", "Rhode Island"); stateMap.put("SC", "South Carolina");
        stateMap.put("SD", "South Dakota"); stateMap.put("TN", "Tennessee"); stateMap.put("TX", "Texas"); stateMap.put("UT", "Utah");
        stateMap.put("VT", "Vermont"); stateMap.put("VA", "Virginia"); stateMap.put("WA", "Washington"); stateMap.put("WV", "West Virginia");
        stateMap.put("WI", "Wisconsin"); stateMap.put("WY", "Wyoming");
        df.sparkSession().udf().register(
                "normalizeState",
                (UDF1<String, String>) location -> {
                    if (location == null) return "Unknown";
                    String loc = location.toUpperCase();
                    for (String full : stateMap.values()) {
                        if (loc.contains(full.toUpperCase())) return full;
                    }
                    for (Map.Entry<String, String> e : stateMap.entrySet()) {
                        if (loc.matches(".*\\b" + e.getKey() + "\\b.*")) return e.getValue();
                    }
                    return "Other";
                },
                DataTypes.StringType
        );
    }


    public static void numeroTotaleTweet(Dataset<Row> df) {
        System.out.println("\n[TOTALE TWEET]");
        System.out.println("Numero totale di tweet: " + df.count());
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


    public static Dataset<Row> distribuzioneTweetPerStato2(Dataset<Row> df) {
        long sogliaBot = 1500;
        Dataset<Row> botList = df.groupBy("user_id_str")
                .count()
                .filter(col("count").gt(sogliaBot))
                .select("user_id_str");
        System.out.println("Numero BOT individuati: " + botList.count());

        //Rimozione bot
        Dataset<Row> cleanDf = df.join(botList, "user_id_str", "left_anti");

        Dataset<Row> filtered = cleanDf.filter(
                col("location").isNotNull()
                        .and(not(col("location").equalTo("NaN")))
                        .and(length(trim(col("location"))).gt(0))
        );
        Dataset<Row> normalized = filtered.withColumn("state",
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
        return normalized
                .filter(col("state").notEqual("Other"))
                .groupBy("state")
                .count()
                .withColumnRenamed("count", "tweet_reali")
                .orderBy(desc("tweet_reali"));
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


    public static Dataset<Row> proporzioneContenuti(Dataset<Row> df) {
        Dataset<Row> contentTypes = df.withColumn("tipo", when(col("text").startsWith("RT @"), "Retweet")
                        .otherwise("Originale"))
                .groupBy("tipo").count();

        return contentTypes;
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


    // Tweet totali per intenzioni di voto (utenti coerenti)
    public static Dataset<Row> hashtagIntenzioniVoto(Dataset<Row> df) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");
        Dataset<Row> tagsExploded = df
                .filter(col("hashtags").isNotNull().and(col("hashtags").notEqual("[]")))
                .withColumn("tag", explode(split(lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")), ",")))
                .withColumn("tag", trim(col("tag")));

        // Utenti coerenti
        Dataset<Row> validUsers = tagsExploded
                .groupBy("user_id_str")
                .agg(
                        count(when(col("tag").isin(bidenTags.toArray()), 1)).as("b_count"),
                        count(when(col("tag").isin(trumpTags.toArray()), 1)).as("t_count")
                )
                .filter((col("b_count").gt(0).and(col("t_count").equalTo(0)))
                        .or(col("t_count").gt(0).and(col("b_count").equalTo(0))));

        Dataset<Row> pureTweets = tagsExploded.join(validUsers, "user_id_str");

        // Mappiamo ogni hashtag al candidato
        Dataset<Row> votoTweets = pureTweets.withColumn("voto_utente",
                        when(col("tag").isin(bidenTags.toArray()), "Biden")
                                .when(col("tag").isin(trumpTags.toArray()), "Trump"))
                .filter(col("voto_utente").isNotNull());

        // Raggruppiamo per candidato e contiamo i tweet
        Dataset<Row> result = votoTweets
                .groupBy("voto_utente")
                .count()
                .withColumnRenamed("count", "tweet_count")
                .orderBy(desc("tweet_count"));

        return result;
    }


    // Utenti unici per intenzioni di voto
    public static Dataset<Row> utentiUniciIntenzioniVoto(Dataset<Row> df) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");
        Dataset<Row> userOrientations = df
                .filter(col("hashtags").isNotNull().and(col("hashtags").notEqual("[]")))
                .withColumn("tag", explode(split(lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")), ",")))
                .withColumn("tag", trim(col("tag")))
                .groupBy("user_id_str")
                .agg(
                        count(when(col("tag").isin(bidenTags.toArray()), 1)).as("b_count"),
                        count(when(col("tag").isin(trumpTags.toArray()), 1)).as("t_count")
                )
                .filter((col("b_count").gt(0).and(col("t_count").equalTo(0)))
                        .or(col("t_count").gt(0).and(col("b_count").equalTo(0))));
        Dataset<Row> result = userOrientations
                .withColumn("elettore", when(col("b_count").gt(0), "Biden").otherwise("Trump"))
                .groupBy("elettore")
                .count()
                .withColumnRenamed("count", "utenti_unici");
        return result;
    }

    public static Dataset<Row> evoluzioneTweetPerCandidato(Dataset<Row> dfTweets) {
        List<String> bidenTags = Arrays.asList("covid19", "bidenharris2020", "biden");
        List<String> trumpTags = Arrays.asList("trump", "maga", "trump2020");

        // Estrazione hashtag e pulizia
        Dataset<Row> tagsExploded = dfTweets
                .filter(col("hashtags").isNotNull().and(not(col("hashtags").equalTo("[]"))))
                .withColumn("tag", explode(split(lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")), ",")))
                .withColumn("tag", trim(col("tag")));

        // Identificazione utenti coerenti: 1 utente = 1 candidato
        Dataset<Row> utentiCoerenti = tagsExploded
                .groupBy("user_id_str")
                .agg(
                        count(when(col("tag").isin(bidenTags.toArray()), 1)).as("b_count"),
                        count(when(col("tag").isin(trumpTags.toArray()), 1)).as("t_count")
                )
                .filter((col("b_count").gt(0).and(col("t_count").equalTo(0)))
                        .or(col("t_count").gt(0).and(col("b_count").equalTo(0))));
        Dataset<Row> pureTweets = tagsExploded.join(utentiCoerenti, "user_id_str")
                .withColumn("voto_utente", when(col("b_count").gt(0), "Biden").otherwise("Trump"))
                .withColumn("date", to_date(col("created_at"), "yyyy-MM-dd"))
                .filter(col("date").isNotNull());
        Dataset<Row> evoluzionePivot = pureTweets
                .groupBy("date")
                .pivot("voto_utente", Arrays.asList("Biden", "Trump"))
                .count()
                .na().fill(0) // se un candidato non ha tweet quel giorno
                .orderBy("date");
        return evoluzionePivot;
    }


    public static Dataset<Row> calcolaVincitorePerStato(Dataset<Row> dfTweets) {
        List<String> bidenTags = Arrays.asList(
                "covid19", "bidenharris2020", "biden"
        );
        List<String> trumpTags = Arrays.asList(
                "trump", "maga", "trump2020"
        );
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
                )
                .filter(col("state_clean").notEqual("Other"))
                .filter(col("user_id_str").isNotNull()); // Assicuriamoci che l'utente esista

        //Estrazione hashtag dai tweet
        Dataset<Row> tagsExploded = dfNormalized
                .withColumn("tag", explode(split(lower(regexp_replace(col("hashtags"), "[\\[\\]\"]", "")), ",")))
                .withColumn("tag", trim(col("tag")));

        // Identificazione orientamento UTENTE (1 persona = 1 voto)
        Dataset<Row> userOrientations = tagsExploded
                .groupBy("user_id_str", "state_clean")
                .agg(
                        count(when(col("tag").isin(bidenTags.toArray()), 1)).as("biden_count"),
                        count(when(col("tag").isin(trumpTags.toArray()), 1)).as("trump_count")
                )
                .filter(
                        (col("biden_count").gt(0).and(col("trump_count").equalTo(0)))
                                .or(col("trump_count").gt(0).and(col("biden_count").equalTo(0)))
                )
                .withColumn("voto_utente", when(col("biden_count").gt(0), "Biden").otherwise("Trump"));

        // Conteggio per Stato
        Dataset<Row> stateSummary = userOrientations
                .groupBy("state_clean")
                .pivot("voto_utente", Arrays.asList("Biden", "Trump"))
                .count()
                .na().fill(0); // Riempie con 0 se non ci sono voti

        // Calcolo ipotetico vincitore
        Dataset<Row> finalResults = stateSummary
                .withColumn("Vincitore", when(col("Biden").gt(col("Trump")), "Biden").otherwise("Trump"))
                .withColumn("Differenza", abs(col("Biden").minus(col("Trump"))))
                .orderBy(asc("state_clean"));

        return finalResults;
    }
}


