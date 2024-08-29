package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.stages.recordpairs.RecordPairsBasic;
import edu.uci.ics.fuzzyjoin.spark.stages.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.spark.stages.tokens.TokensBasic;
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
        Date startTime = new Date();
        String[] tokensRank = StartTokensBasic.start(sc, records);
        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "TokensBasic");

        //
        // Stage 2 : Finding RID pairs
        //
        startTime = new Date();
        JavaRDD<String> ridPairs = StartRidPairsPPJoin.start(sc, records, tokensRank);
        endTime = new Date();
        LogUtil.logTime(startTime, endTime, "RIDPairsPPJoin");

        //
        // Stage 3 : Record pairing
        //
        startTime = new Date();
        StartRecordPairsBasic.start(sc, records, ridPairs);
        endTime = new Date();
        LogUtil.logTime(startTime, endTime, "RecordPairsBasic");

        // saveResult();
    }
}
