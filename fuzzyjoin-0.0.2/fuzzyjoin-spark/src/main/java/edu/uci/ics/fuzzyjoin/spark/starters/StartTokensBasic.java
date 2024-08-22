package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.logging.SaveResult;
import edu.uci.ics.fuzzyjoin.spark.tokens.TokensBasic;

public class StartTokensBasic {
    public static String[] start(JavaSparkContext sc, boolean saveResult) throws IOException {
        //
        // Read files from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read raw data from HDFS");
        JavaRDD<String> raw = configuration.readData(sc, "raw");

        //
        // Launch Stage 1 : Tokenization
        //

        LogUtil.logStage("Start Stage 1 : TokensBasic");
        JavaPairRDD<Integer, String> tokensRank = TokensBasic.main(raw, sc);

        if (saveResult) {
            // Save the result in HDFS
            SaveResult saver = new SaveResult(sc, "tokens");
            saver.saveJavaRDD(tokensRank.map(t -> t._2()));
        }

        return tokensRank.values().collect().toArray(new String[0]);
    }
}
