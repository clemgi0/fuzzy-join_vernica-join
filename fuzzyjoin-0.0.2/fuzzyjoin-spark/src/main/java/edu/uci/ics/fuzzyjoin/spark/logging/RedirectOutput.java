package edu.uci.ics.fuzzyjoin.spark.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class RedirectOutput {
    public static void setFile(String fileName) {
        try {
            // Création d'un objet PrintStream pointant vers le fichier
            PrintStream fileOut = new PrintStream(new File(fileName));

            // Redirection de System.out vers le fichier
            System.setOut(fileOut);

            // Exemple de sortie redirigée vers le fichier
            LogUtil.logStage("Début du logging");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void setConsole() {
        try {
            LogUtil.logStage("Fin du logging");

            // Redirection de System.out vers la console
            System.setOut(new PrintStream(System.out));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
