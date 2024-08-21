package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

public class StartFuzzyJoin {
    public static void startFuzzyJoin() throws IOException {
        String[] tokensRank = StartTokensBasic.startTokensBasic(false);
        StartRidPairsPPJoin.startRidPairsPPJoin(false, tokensRank);
        // StartRecordPairsBasic.startRecordPairsBasic(false, ridPairs);
        // saveResult();
    }
}
