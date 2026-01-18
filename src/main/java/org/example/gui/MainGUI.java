package org.example.gui;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class MainGUI extends Application {

    @Override
    public void start(Stage primaryStage) throws Exception {
        // Carica il FXML dal percorso delle risorse
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/org/example/gui/homeView.fxml"));
        System.out.println("FXML caricato correttamente");
        Scene scene = new Scene(loader.load(), 800, 600);
        primaryStage.setTitle("Twitter Spark Analysis GUI");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    public static void main(String[] args) {
        try {
            launch(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
