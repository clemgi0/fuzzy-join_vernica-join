package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;

public class StartFuzzyJoin {
    public static void start(JavaSparkContext sc) throws IOException {
        //
        // Read files from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records data from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        String[] tokensRank = StartTokensBasic.start(sc, records);
        JavaRDD<String> ridPairs = StartRidPairsPPJoin.start(sc, records, tokensRank);
        StartRecordPairsBasic.start(sc, records, ridPairs);
        // saveResult();
    }
}
