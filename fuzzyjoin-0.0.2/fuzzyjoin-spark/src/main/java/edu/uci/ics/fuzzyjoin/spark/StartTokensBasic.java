package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.tokens.TokensBasic;

public class StartTokensBasic {
    public static String[] startTokensBasic(boolean saveResult) throws IOException {
        //
        // Read files from HDFS
        //

        LogUtil.logStage("Read raw data from HDFS");
        JavaRDD<String> raw = Main.configuration.readData(Main.sc, "raw");

        //
        // Launch Stage 1 : Tokenization
        //

        LogUtil.logStage("Start Stage 1 : TokensBasic");
        JavaPairRDD<Integer, String> tokensRank = TokensBasic.main(raw, Main.sc);

        // if (saveResult) {
        // saveResult(tokensRank);
        // }

        return tokensRank.values().collect().toArray(new String[0]);
    }
}
