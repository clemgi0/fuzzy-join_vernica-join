package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;

import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.RIDPairsPPJoin;

public class StartRidPairsPPJoin {
    public static void startRidPairsPPJoin(boolean saveResult, String[] tokensRank) throws IOException {
        //
        // Read records from HDFS
        //

        LogUtil.logStage("Read records from HDFS");
        JavaRDD<String> records = Main.configuration.readData(Main.sc, "records");

        if (saveResult) {
            JavaRDD<String> tokensRankRDD = Main.configuration.readData(Main.sc, "tokens");
            tokensRank = tokensRankRDD.collect().toArray(new String[0]);
        }

        //
        // Launch Stage 2 : FuzzyJoin
        //

        LogUtil.logStage("Start Stage 2 : RIDPairsPPJoin");
        RIDPairsPPJoin.main(tokensRank, records, Main.sc);

        // if (saveResult) {
        // saveResult();
        // }
    }
}
