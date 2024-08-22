package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.logging.SaveResult;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.IntPair;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin;

public class StartRidPairsPPJoin {
    public static void start(JavaSparkContext sc, boolean saveResult, String[] tokensRank)
            throws IOException {
        //
        // Read records from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        if (saveResult) {
            JavaRDD<String> tokensRankRDD = configuration.readData(sc, "tokens");
            tokensRank = tokensRankRDD.collect().toArray(new String[0]);
        }

        //
        // Launch Stage 2 : FuzzyJoin
        //

        LogUtil.logStage("Start Stage 2 : RIDPairsPPJoin");
        JavaPairRDD<IntPair, ValueSelfJoin> ridPairs = RIDPairsPPJoin.main(tokensRank, records, sc);

        if (saveResult) {
            SaveResult saver = new SaveResult(sc, "ridpairs");
            saver.saveJavaRIDPairRDD(ridPairs);
        }
    }
}
