package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;

public class StartFuzzyJoin {
    public static void start(JavaSparkContext sc) throws IOException {

        String[] tokensRank = StartTokensBasic.start(sc, false);
        StartRidPairsPPJoin.start(sc, false, tokensRank);

        // StartRecordPairsBasic.startRecordPairsBasic(false, ridPairs);
        // saveResult();

    }
}
