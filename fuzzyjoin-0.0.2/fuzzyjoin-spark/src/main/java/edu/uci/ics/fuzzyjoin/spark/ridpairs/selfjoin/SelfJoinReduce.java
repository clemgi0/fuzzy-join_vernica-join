package edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;

public class SelfJoinReduce implements FlatMapFunction<Iterable<ValueSelfJoin>, String> {
    private float similarityThreshold;

    public SelfJoinReduce(JavaSparkContext sc) {
        similarityThreshold = (float) sc.getConf().getDouble(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
    }

    @Override
    public Iterator<String> call(Iterable<ValueSelfJoin> values) throws Exception {
        HashMap<Integer, Integer> rids = new HashMap<Integer, Integer>();
        FuzzyJoinMemory fuzzyJoinMemory = new FuzzyJoinMemory(similarityThreshold);
        int crtRecordId = 0;

        List<String> ret = new ArrayList<>();

        for (ValueSelfJoin value : values) {
            int[] record = value.getTokens();
            rids.put(crtRecordId, value.getRID());
            crtRecordId++;

            ArrayList<ResultSelfJoin> results = fuzzyJoinMemory.selfJoinAndAddRecord(record);

            for (ResultSelfJoin result : results) {
                int rid1 = rids.get(result.indexX);
                int rid2 = rids.get(result.indexY);
                if (rid1 < rid2) {
                    int rid = rid1;
                    rid1 = rid2;
                    rid2 = rid;
                }

                ret.add("" + rid1 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                        + rid2 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                        + String.valueOf(result.similarity));
            }
        }

        return ret.iterator();
    }
}
