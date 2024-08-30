package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.util.LogUtil;

public class StartFuzzyJoin {
    public static void start(JavaSparkContext sc) throws IOException {
        //
        // Read files from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records data from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        //
        // Stage 1 : Tokenization
        //
        String[] tokensRank = StartTokensBasic.start(sc, records);

        //
        // Stage 2 : Finding RID pairs
        //
        JavaRDD<String> ridPairs = StartRidPairsPPJoin.start(sc, records, tokensRank);

        //
        // Stage 3 : Record pairing
        //
        StartRecordPairsBasic.start(sc, records, ridPairs);
    }
}
