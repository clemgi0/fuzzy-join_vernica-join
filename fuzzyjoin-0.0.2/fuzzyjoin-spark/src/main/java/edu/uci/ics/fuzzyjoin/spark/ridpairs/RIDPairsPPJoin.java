package edu.uci.ics.fuzzyjoin.spark.ridpairs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.SelfJoinMap;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.SelfJoinReduce;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin.ValueSelfJoin;
import scala.Tuple2;

public class RIDPairsPPJoin {
    public static JavaPairRDD<Integer, ValueSelfJoin> main(String[] tokenStrings,
            JavaRDD<String> records, JavaSparkContext sc)
            throws IOException {

        JavaPairRDD<Integer, String> ridPairs;
        JavaPairRDD<Integer, ValueSelfJoin> selfJoinMappedData;

        if (sc.getConf().get(Main.DATA_SUFFIX_INPUT_PROPERTY).isEmpty()) {
            //
            // self-join
            //

            LogUtil.logStage("Self-join : Map");
            selfJoinMappedData = records.flatMapToPair(new SelfJoinMap(sc, tokenStrings));

            // showPairRDD(selfJoinMappedData);

            LogUtil.logStage("Self-join : Reduce");
            JavaPairRDD<Integer, Iterable<ValueSelfJoin>> selfJoinGroupedData = selfJoinMappedData.groupByKey();

            // Sort the RDD by keys (IntPair)
            JavaPairRDD<Integer, Iterable<ValueSelfJoin>> sortedSelfJoinGroupedData = selfJoinGroupedData.sortByKey();

            // showPairRDD2(sortedSelfJoinGroupedData);

            ridPairs = sortedSelfJoinGroupedData.flatMapValues(new SelfJoinReduce(sc));

            showPairRDD3(ridPairs);
        } else {
            //
            // R-S join
            //

            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE");
            LogUtil.logStage("R-S join : Map");
            // job.setMapperClass(MapJoin

            LogUtil.logStage("R-S join : Reduce");
            // job.setReducerClass(ReduceJoin

            selfJoinMappedData = records
                    .flatMapToPair(new SelfJoinMap(sc, tokenStrings)); // TO DELETE CUZ IT S JUST TO AVOID A NULL RETURN
                                                                       // IN THE NEXT LINE
        }

        return selfJoinMappedData;
    }

    public static void showPairRDD(JavaPairRDD<Integer, ValueSelfJoin> rdd) {
        List<Tuple2<Integer, ValueSelfJoin>> results = rdd.collect();
        results.forEach(r -> System.out
                .println("Group : " + r._1() + "  ||  RID : "
                        + r._2().getRID() + " TokensRanked : " + Arrays.toString(r._2().getTokens())));
    }

    public static void showPairRDD2(JavaPairRDD<Integer, Iterable<ValueSelfJoin>> rdd) {
        List<Tuple2<Integer, Iterable<ValueSelfJoin>>> results = rdd.collect();
        for (Tuple2<Integer, Iterable<ValueSelfJoin>> r : results) {
            System.out.println("Group : " + r._1() + "  ||  RIDs : ");
            for (ValueSelfJoin v : r._2()) {
                System.out.println("\t- RID : " + v.getRID() + " | TokensRanked : " + Arrays.toString(v.getTokens()));
            }
        }
    }

    public static void showPairRDD3(JavaPairRDD<Integer, String> rdd) {
        List<Tuple2<Integer, String>> results = rdd.collect();
        results.forEach(r -> System.out
                .println("" + r._2()));
    }
}
