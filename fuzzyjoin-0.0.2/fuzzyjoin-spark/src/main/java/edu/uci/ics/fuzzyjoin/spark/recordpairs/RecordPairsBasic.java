package edu.uci.ics.fuzzyjoin.spark.recordpairs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.objects.IntPair;
import edu.uci.ics.fuzzyjoin.spark.recordpairs.selfjoin.SelfJoinPhase1Map;
import edu.uci.ics.fuzzyjoin.spark.recordpairs.selfjoin.SelfJoinPhase1Reduce;
import scala.Tuple2;

public class RecordPairsBasic {
    public static void main(JavaSparkContext sc, JavaRDD<String> records, JavaRDD<String> ridPairs) throws IOException {
        //
        // -------------------- PHASE 1 --------------------
        //

        JavaPairRDD<IntPair, String> halfRecords;

        if (sc.getConf().get(Main.DATA_SUFFIX_INPUT_PROPERTY).isEmpty()) {
            //
            // self-join
            //

            JavaRDD<String> union = records.union(ridPairs);

            // showRDD1(union);

            LogUtil.logStage("Self-join : Map");

            JavaPairRDD<String, Integer> mappedData = union.flatMapToPair(new SelfJoinPhase1Map());
            JavaPairRDD<Integer, String> mappedDataInverted = mappedData.mapToPair(Tuple2::swap);

            showPairRDD1(mappedDataInverted);

            LogUtil.logStage("Self-join : Reduce");

            JavaPairRDD<Integer, Iterable<String>> mappedDataInvertedGrouped = mappedDataInverted.groupByKey();
            List<Tuple2<Integer, Iterable<String>>> mappedDataIterable = mappedDataInvertedGrouped.collect();

            ArrayList<Tuple2<IntPair, String>> halfRecordsList = new ArrayList<>();
            for (Tuple2<Integer, Iterable<String>> tuple : mappedDataIterable) {
                System.out.println(tuple._1() + " " + tuple._2());
                halfRecordsList.addAll(new SelfJoinPhase1Reduce().call(tuple));
            }

            halfRecords = sc.parallelizePairs(halfRecordsList);
        } else {
            //
            // R-S join
            //

            halfRecords = new JavaPairRDD<>(null, null, null);
        }

        showPairRDD2(halfRecords);

        //
        // -------------------- PHASE 2 --------------------
        //

        if (sc.getConf().get(Main.DATA_SUFFIX_INPUT_PROPERTY).isEmpty()) {
            //
            // self-join
            //

        } else {
            //
            // R-S join
            //

        }
    }

    public static void showRDD1(JavaRDD<String> rdd) {
        List<String> results = rdd.collect();
        results.forEach(r -> System.out
                .println("" + r));
    }

    public static void showPairRDD1(JavaPairRDD<Integer, String> rdd) {
        List<Tuple2<Integer, String>> results = rdd.collect();
        results.forEach(r -> System.out
                .println(r._1() + " " + r._2()));
    }

    public static void showPairRDD2(JavaPairRDD<IntPair, String> rdd) {
        List<Tuple2<IntPair, String>> results = rdd.collect();
        results.forEach(r -> System.out
                .println(r._1().getFirst() + " " + r._1().getSecond() + " " + r._2()));
    }
}
