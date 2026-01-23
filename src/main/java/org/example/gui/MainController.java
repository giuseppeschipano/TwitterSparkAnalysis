package org.example.gui;

import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.chart.*;
import javafx.scene.control.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.query.BasicQuery;
import org.example.spark.SparkSessionProvider;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.example.loader.DatasetLoader;

public class MainController {

    private static final int MAX_ROWS = 250;

    @FXML private ComboBox<String> querySelector;
    @FXML private Button executeButton;
    @FXML private TextArea logArea;
    @FXML private TableView<Map<String, Object>> table;
    @FXML private BarChart<String, Number> barChart;
    @FXML private CategoryAxis xAxis;
    @FXML private NumberAxis yAxis;
    @FXML private PieChart pieChart;
    @FXML private LineChart<String, Number> lineChart;
    @FXML private CategoryAxis lineXAxis;
    @FXML private NumberAxis lineYAxis;


    private SparkSession spark;
    private Dataset<Row> df;

    @FXML
    private void initialize() {
        executeButton.setDisable(true);
        logArea.appendText("Avvio Spark in background...\n");

        // Apertura Spark in un thread separato
        new Thread(() -> {
            // Creazione SparkSession
            spark = SparkSessionProvider.createSession();
            Platform.runLater(() -> logArea.appendText("Sessione Spark creata.\n"));
            try {
                // Creazione del DatasetLoader
                DatasetLoader loader = new DatasetLoader(spark, "D:\\TwitterSparkAnalysis\\parquet\\tweets_usa_2020");
                // Caricamente del dataset Parquet preprocessato (colonnare e molto più veloce dei CSV)
                df = loader.loadDataset();
                if (df == null) {
                    Platform.runLater(() -> logArea.appendText("Errore: dataset non disponibile.\n"));
                    return;
                }
                Platform.runLater(() -> {
                    logArea.appendText("Dataset caricato con successo.\n" + "Ora puoi selezionare ed eseguire le query.\n");
                    executeButton.setDisable(false);
                });
            } catch (Exception e) {
                Platform.runLater(() -> logArea.appendText("Errore caricamento dataset: " + e.getMessage() + "\n"));
            }
        }).start();
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        querySelector.getItems().addAll(
                "Numero di tweet considerati",
                "Distribuzione tweet per stato",
                "Distribuzione tweet reali per stato",
                "Andamento giornaliero tweet",
                "Proporzione contenuti",
                "Top hashtag",
                "Top hashtag per stato",
                "Intenzioni di voto",
                "Utenti unici intenzioni di voto",
                "Evoluzione tweet per candidato",
                "Calcola vincitore per stato"
        );
        logArea.appendText("GUI pronta, seleziona una query.\n");
        // Collegamento del bottone
        executeButton.setOnAction(e -> executeQuery());
    }


    private void executeQuery() {
        String query = querySelector.getValue();
        if (query == null || df == null) {
            logArea.appendText("Seleziona una query valida e attendi il caricamento del dataset.\n");
            return;
        }
        logArea.appendText("\nEsecuzione query: " + query + "\n");
        try {
            Dataset<Row> result = null;

            switch (query) {

                case "Numero totale tweet":
                    long total = df.count();
                    logArea.appendText("[TOTALE TWEET]\n");
                    logArea.appendText("Numero totale di tweet nel dataset: " + total + "\n\n");
                    Map<String, Object> row = new HashMap<>();
                    row.put("Descrizione", "Numero totale di tweet considerati");
                    row.put("Valore", total);
                    ObservableList<Map<String, Object>> items = FXCollections.observableArrayList(row);
                    table.getColumns().clear();
                    TableColumn<Map<String, Object>, String> colDescrizione = new TableColumn<>("Descrizione");
                    colDescrizione.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(data.getValue().get("Descrizione").toString()));
                    TableColumn<Map<String, Object>, String> colValore = new TableColumn<>("Valore");
                    colValore.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(data.getValue().get("Valore").toString()));
                    table.getColumns().addAll(colDescrizione, colValore);
                    table.setItems(items);
                    break;

                case "Distribuzione tweet per stato":
                    result = BasicQuery.distribuzioneTweetPerStato1(df);
                    populateTable(result);
                    Map<String, Number> map = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> map.put(r.getAs("normalized_state"), (Number) r.getAs("count")));
                    updateBarChart("Tweet per Stato", "Stato", map);
                    break;


                case "Distribuzione tweet reali per stato":
                    result = BasicQuery.distribuzioneTweetPerStato2(df);
                    populateTable(result);
                    Map<String, Number> graficoData = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> graficoData.put(r.getAs("state"), (Number) r.getAs("tweet_reali")));
                    updateBarChart("Tweet reali per stato", "Stato", graficoData);
                    break;

                case "Andamento giornaliero tweet":
                    result = BasicQuery.andamentoGiornalieroTweet(df);
                    populateTable(result);
                    Map<String, Map<String, Number>> lineData = new LinkedHashMap<>();
                    Map<String, Number> serie = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> serie.put(r.getAs("date").toString(), (Number) r.getAs("count")));
                    lineData.put("Tweet giornalieri", serie);
                    updateLineChart(
                            "Andamento giornaliero dei tweet",
                            "Data",
                            "Numero tweet",
                            lineData
                    );
                    break;


                case "Proporzione contenuti":
                    result = BasicQuery.proporzioneContenuti(df);
                    populateTable(result);
                    Map<String, Number> pieData = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> pieData.put(r.getAs("tipo"), (Number) r.getAs("count")));
                    updatePieChart("Proporzione contenuti", pieData);
                    break;

                case "Top hashtag":
                    result = BasicQuery.topHashtag(df);
                    populateTable(result);
                    Map<String, Number> pieTopHashtag = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> pieTopHashtag.put(r.getAs("tag"), (Number) r.getAs("count")));
                    updatePieChart("Top 10 Hashtag", pieTopHashtag);
                    break;

                case "Top hashtag per stato":
                    Dataset<Row> resultTop = BasicQuery.topHashtagPerStato(df);
                    populateTable(resultTop);
                    updateTopHashtagPerStatoBarChart(
                            "Top 5 Hashtag per Stato",
                            "Stato",
                            "Numero Tweet",
                            resultTop
                    );
                    break;

                case "Intenzioni di voto":
                    result = BasicQuery.hashtagIntenzioniVoto(df);
                    populateTable(result);
                    Map<String, Number> pieDataVote = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> pieDataVote.put(r.getAs("voto_utente"), (Number) r.getAs("tweet_count")));
                    updatePieChart("Tweet per candidato (coerenti)", pieDataVote);
                    break;

                case "Utenti unici intenzioni di voto":
                    result = BasicQuery.utentiUniciIntenzioniVoto(df);
                    populateTable(result);
                    Map<String, Number> pieDataUnique = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> pieDataUnique.put(r.getAs("elettore"), (Number) r.getAs("utenti_unici")));
                    updatePieChart("Utenti unici coerenti per candidato", pieDataUnique);
                    break;

                case "Evoluzione tweet per candidato":
                    result = BasicQuery.evoluzioneTweetPerCandidato(df);
                    populateTable(result);
                    Map<String, Map<String, Number>> lineChartData = new LinkedHashMap<>();
                    result.collectAsList().forEach(r -> {
                        String candidato = r.getAs("voto_utente");
                        String date = r.getAs("date").toString();
                        Number count = (Number) r.getAs("count");
                        lineChartData.putIfAbsent(candidato, new LinkedHashMap<>());
                        lineChartData.get(candidato).put(date, count);
                    });
                    updateLineChart("Evoluzione Tweet per candidato", "Data", "Numero Tweet", lineChartData);
                    break;

                case "Calcola vincitore per stato":
                    Dataset<Row> vincitorePerStato = BasicQuery.calcolaVincitorePerStato(df);
                    populateTable(vincitorePerStato);
                    Map<String, Number> diffMap = new LinkedHashMap<>();
                    vincitorePerStato.collectAsList().forEach(r -> diffMap.put(r.getAs("state_clean"), (Number) r.getAs("Differenza")));
                    updateBarChart("Differenza voti per stato", "Stato", diffMap);
                    Map<String, Number> pieDataVincitori = new LinkedHashMap<>();
                    vincitorePerStato.groupBy("Vincitore").count().collectAsList().forEach(r -> pieDataVincitori.put(r.getAs("Vincitore"), (Number) r.getAs("count")));
                    updatePieChart("Numero stati vinti per candidato", pieDataVincitori);
                    break;
                default:
                    logArea.appendText("Query eseguita. Controlla console per i dettagli.\n");
            }
        } catch (Exception ex) {
            logArea.appendText("Errore esecuzione query: " + ex.getMessage() + "\n");
        }
    }


    private void populateTable(Dataset<Row> dataset) {
        table.getColumns().clear();
        List<String> columns = List.of(dataset.columns());
        for (String colName : columns) {
            TableColumn<Map<String, Object>, String> col = new TableColumn<>(colName);
            col.setCellValueFactory(data -> {
                Object value = data.getValue().get(colName);
                return new javafx.beans.property.SimpleStringProperty(value != null ? value.toString() : "");
            });
            table.getColumns().add(col);
        }
        Dataset<Row> limitedDataset = dataset.limit(MAX_ROWS);
        List<Map<String, Object>> rows = limitedDataset.collectAsList().stream()
                .map(row -> {
                    Map<String, Object> map = new HashMap<>();
                    for (String colName : columns) {
                        map.put(colName, row.getAs(colName));
                    }
                    return map;
                }).toList();
        ObservableList<Map<String, Object>> items = FXCollections.observableArrayList(rows);
        table.setItems(items);
        if (dataset.count() > MAX_ROWS) {
            logArea.appendText("Visualizzate solo le prime " + MAX_ROWS + " righe.\n");
        }
    }

    private void updateBarChart(String title, String categoryLabel, Map<String, Number> data) {
        barChart.setTitle(title);
        xAxis.setLabel(categoryLabel);
        yAxis.setLabel("Conteggio");
        barChart.getData().clear();
        javafx.scene.chart.XYChart.Series<String, Number> series = new javafx.scene.chart.XYChart.Series<>();
        data.forEach((cat, val) -> series.getData().add(new javafx.scene.chart.XYChart.Data<>(cat, val)));
        barChart.getData().add(series);
    }

    private void updatePieChart(String title, Map<String, Number> data) {
        Platform.runLater(() -> {
            pieChart.setTitle(title);
            pieChart.getData().clear();
            data.forEach((label, value) -> pieChart.getData().add(new PieChart.Data(label, value.doubleValue())));
            pieChart.setClockwise(true);
            pieChart.setLabelsVisible(true);
            pieChart.setLegendVisible(true);
        });
    }


    private void updateLineChart(String title, String xLabel, String yLabel, Map<String, Map<String, Number>> data) {
        Platform.runLater(() -> {
            lineChart.setTitle(title);
            lineXAxis.setLabel(xLabel);
            lineYAxis.setLabel(yLabel);
            lineChart.getData().clear();
            data.forEach((seriesName, map) -> {
                XYChart.Series<String, Number> series = new XYChart.Series<>();
                series.setName(seriesName);
                map.forEach((x, y) -> series.getData().add(new XYChart.Data<>(x, y)));
                lineChart.getData().add(series);
            });
        });
    }

    private void updateTopHashtagPerStatoBarChart(
            String title,
            String xLabel,
            String yLabel,
            Dataset<Row> dataset
    ) {
        Platform.runLater(() -> {
            barChart.setTitle(title);
            xAxis.setLabel(xLabel);
            yAxis.setLabel(yLabel);
            barChart.getData().clear();
            Map<String, Map<String, Number>> data = new LinkedHashMap<>();
            dataset.collectAsList().forEach(r -> {
                String stato = r.getAs("state_clean");
                String hashtag = r.getAs("tag");
                Number count = (Number) r.getAs("count");
                data.computeIfAbsent(hashtag, k -> new LinkedHashMap<>()).put(stato, count);
            });
            data.forEach((hashtag, map) -> {
                XYChart.Series<String, Number> series = new XYChart.Series<>();
                series.setName(hashtag);
                map.forEach((stato, value) -> {
                    series.getData().add(new XYChart.Data<>(stato, value));
                });
                barChart.getData().add(series);
            });
        });
    }
}






