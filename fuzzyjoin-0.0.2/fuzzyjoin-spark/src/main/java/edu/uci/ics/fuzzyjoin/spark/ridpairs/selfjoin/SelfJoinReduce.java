package edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.IntPair;
import scala.Tuple2;

public class SelfJoinReduce implements FlatMapFunction<Iterable<ValueSelfJoin>, List<String>> {
    private float similarityThreshold;
    private FuzzyJoinMemory fuzzyJoinMemory;
    private HashMap<Integer, Integer> rids;
    private int crtRecordId;
    private JavaSparkContext sc;

    public SelfJoinReduce(JavaSparkContext sc) {
        similarityThreshold = (float) sc.getConf().getDouble(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);

        rids = new HashMap<Integer, Integer>();
        fuzzyJoinMemory = new FuzzyJoinMemory(similarityThreshold);
        crtRecordId = 0;
        this.sc = sc;
    }

    @Override
    public JavaPairRDD<IntPair, List<String>> call(Iterable<ValueSelfJoin> values)
            throws Exception {
        List<String> ret = new ArrayList<String>();

        for (ValueSelfJoin value : values) {
            int[] record1 = value.getTokens();
            rids.put(crtRecordId, value.getRID());
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
        }

        // Convert List<String> to List<Tuple2<IntPair, List<String>>>
        List<Tuple2<IntPair, List<String>>> resultTuples = ret.stream()
                .map(s -> new Tuple2<>(new IntPair(), Arrays.asList(s)))
                .collect(Collectors.toList());

        // Parallelize the list of tuples and create a JavaPairRDD
        JavaRDD<Tuple2<IntPair, List<String>>> rdd = sc.parallelize(resultTuples);

        return rdd.map(tuple -> tuple._2).collect();
    }
}
