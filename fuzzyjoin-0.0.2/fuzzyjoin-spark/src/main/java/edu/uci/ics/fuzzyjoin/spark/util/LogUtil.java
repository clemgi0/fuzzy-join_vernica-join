package edu.uci.ics.fuzzyjoin.spark.util;

import java.util.Date;

public class LogUtil {
    public static void logStage(String stage) {
        System.out.println();
        System.out.println("-------------------- " + stage + " --------------------");
        System.out.println();
    }

    public static void logTime(Date startTime, Date endTime, String stage) {
        System.out.println();
        System.out.println("------- The stage " + stage + " took "
                + (endTime.getTime() - startTime.getTime()) / (float) 1000.0 + " seconds.");
        System.out.println();
    }
}
