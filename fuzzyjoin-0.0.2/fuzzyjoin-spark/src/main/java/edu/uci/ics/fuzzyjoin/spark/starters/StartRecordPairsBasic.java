package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.stages.recordpairs.RecordPairsBasic;
import edu.uci.ics.fuzzyjoin.spark.util.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.util.SaveResult;

public class StartRecordPairsBasic {
    public static void start(JavaSparkContext sc)
            throws IOException {
        //
        // Read records from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records data from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        //
        // Read records from HDFS
        //
        LogUtil.logStage("Read ridpairs data from HDFS");
        JavaRDD<String> ridPairs = configuration.readData(sc, "ridpairs");

        //
        // Launch Stage 3 : Similar records join
        //
        LogUtil.logStage("Start Stage 3 : RecordsPairsBasic");
        Date startTime = new Date();

        JavaRDD<String> pairedRecords = RecordPairsBasic.main(sc, records, ridPairs);

        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "recordpairs");

        //
        // Save the result in HDFS
        //
        SaveResult saver = new SaveResult(sc, "recordpairs");
        saver.saveJavaStringRDD(pairedRecords);
    }

    public static void start(JavaSparkContext sc, JavaRDD<String> records, JavaRDD<String> ridPairs)
            throws IOException {
        //
        // Launch Stage 3 : Similar records join
        //
        LogUtil.logStage("Start Stage 3 : RecordsPairsBasic");
        Date startTime = new Date();

        JavaRDD<String> pairedRecords = RecordPairsBasic.main(sc, records, ridPairs);

        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "recordpairs");

        //
        // Save the result in HDFS
        //
        SaveResult saver = new SaveResult(sc, "recordpairs");
        saver.saveJavaStringRDD(pairedRecords);
    }
}
