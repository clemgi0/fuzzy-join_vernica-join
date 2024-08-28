package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.logging.SaveResult;
import edu.uci.ics.fuzzyjoin.spark.tokens.TokensBasic;

public class StartTokensBasic {
    public static void start(JavaSparkContext sc) throws IOException {
        //
        // Read files from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records data from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        //
        // Launch Stage 1 : Tokenization
        //

        LogUtil.logStage("Start Stage 1 : TokensBasic");
        JavaRDD<String> tokensRank = TokensBasic.main(records, sc);

        // Save the result in HDFS
        SaveResult saver = new SaveResult(sc, "tokens");
        saver.saveJavaStringRDD(tokensRank);
    }

    public static String[] start(JavaSparkContext sc, JavaRDD<String> records) throws IOException {
        //
        // Launch Stage 1 : Tokenization
        //

        LogUtil.logStage("Start Stage 1 : TokensBasic");
        JavaRDD<String> tokensRank = TokensBasic.main(records, sc);

        return tokensRank.collect().toArray(new String[0]);
    }
}
