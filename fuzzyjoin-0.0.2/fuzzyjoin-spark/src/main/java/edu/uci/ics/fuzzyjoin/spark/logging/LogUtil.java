package edu.uci.ics.fuzzyjoin.spark.logging;

public class LogUtil {
    public static void logStage(String stage) {
        System.out.println();
        System.out.println("-------------------- " + stage + " --------------------");
        System.out.println();
    }
}
