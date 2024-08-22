package edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;

public class SelfJoinReduce implements
        Function2<edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin, edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin, List<String>> {
    private float similarityThreshold;
    private String outputKey;
    private FuzzyJoinMemory fuzzyJoinMemory;
    private HashMap<Integer, Integer> rids;
    private int crtRecordId;

    public SelfJoinReduce(JavaSparkContext sc) {
        similarityThreshold = (float) sc.getConf().getDouble(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);

        rids = new HashMap<Integer, Integer>();
        fuzzyJoinMemory = new FuzzyJoinMemory(similarityThreshold);
        crtRecordId = 0;
    }

    @Override
    public List<String> call(edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin value1,
            edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin value2)
            throws Exception {
        List<String> ret = new ArrayList<String>();

        // Handle first record
        int[] record1 = value1.getTokens();
        rids.put(crtRecordId, value1.getRID());
        crtRecordId++;

        ArrayList<ResultSelfJoin> results1 = fuzzyJoinMemory
                .selfJoinAndAddRecord(record1);
        for (ResultSelfJoin result : results1) {
            int rid1 = rids.get(result.indexX);
            int rid2 = rids.get(result.indexY);
            if (rid1 < rid2) {
                int rid = rid1;
                rid1 = rid2;
                rid2 = rid;
            }

            ret.add(rid1 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR + rid2 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + String.valueOf(result.similarity));
        }

        // Handle second record
        int[] record2 = value2.getTokens();
        rids.put(crtRecordId, value2.getRID());
        crtRecordId++;

        ArrayList<ResultSelfJoin> results2 = fuzzyJoinMemory
                .selfJoinAndAddRecord(record2);
        for (ResultSelfJoin result : results2) {
            int rid1 = rids.get(result.indexX);
            int rid2 = rids.get(result.indexY);
            if (rid1 < rid2) {
                int rid = rid1;
                rid1 = rid2;
                rid2 = rid;
            }

            ret.add(rid1 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR + rid2 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + String.valueOf(result.similarity));
        }

        return ret;
    }
}
