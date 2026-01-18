package org.example.gui;

import javafx.animation.PauseTransition;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.ImageView;
import javafx.stage.Stage;
import javafx.util.Duration;

import java.io.IOException;

public class HomeViewController {

    @FXML
    private ImageView splashImage;

    @FXML
    public void initialize() {
        // Mostra  per 10 secondi
        PauseTransition delay = new PauseTransition(Duration.seconds(10));
        delay.setOnFinished(event -> {
            openMainWindow();
            // Chiusura splash
            splashImage.getScene().getWindow().hide();
        });
        delay.play();
    }

    private void openMainWindow() {
        try {
            FXMLLoader loader = new FXMLLoader(getClass().getResource("main.fxml"));
            Scene scene = new Scene(loader.load());
            Stage stage = new Stage();
            stage.setTitle("Analisi Tweet");
            stage.setScene(scene);
            stage.show();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
