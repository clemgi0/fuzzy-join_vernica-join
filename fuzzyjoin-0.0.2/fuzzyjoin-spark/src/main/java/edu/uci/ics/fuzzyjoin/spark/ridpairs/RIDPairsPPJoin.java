package edu.uci.ics.fuzzyjoin.spark.ridpairs;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin.SelfJoinMap;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin.ValueSelfJoin;
import scala.Tuple2;

public class RIDPairsPPJoin {
    public static void main(String[] tokenStrings,
            JavaRDD<String> records, JavaSparkContext sc)
            throws IOException {

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>> tokensGrouped;
        JavaPairRDD<IntPair, ValueSelfJoin> selfJoinMappedData;

        if (sc.getConf().get(Main.DATA_SUFFIX_INPUT_PROPERTY).isEmpty()) {
            // self-join

            LogUtil.logStage("Self-join : Map");

            selfJoinMappedData = records
                    .flatMapToPair(new SelfJoinMap(sc, tokenStrings));

            showPairRDD(selfJoinMappedData);

            LogUtil.logStage("Self-join : Reduce");
            // job.setReducerClass(ReduceSelfJoin

        } else {
            // R-S join

            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE");
            LogUtil.logStage("R-S join : Map");
            // job.setMapperClass(MapJoin

            LogUtil.logStage("R-S join : Reduce");
            // job.setReducerClass(ReduceJoin
        }

    }

    public static void showPairRDD(JavaPairRDD<IntPair, ValueSelfJoin> rdd) {
        List<Tuple2<IntPair, ValueSelfJoin>> results = rdd.collect();
        results.forEach(r -> System.out
                .println("Group : " + r._1().getFirst() + " | Length : " + r._1().getSecond() + "  ||  RID : "
                        + r._2().getRID() + " TokensRanked : " + Arrays.toString(r._2().getTokens())));
    }
}
