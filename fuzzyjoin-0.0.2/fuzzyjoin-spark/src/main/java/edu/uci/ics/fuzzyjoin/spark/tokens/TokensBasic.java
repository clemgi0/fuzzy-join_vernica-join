package edu.uci.ics.fuzzyjoin.spark.tokens;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.tokens.Scalar.ScalarMap;
import edu.uci.ics.fuzzyjoin.spark.tokens.Scalar.ScalarReduceAggregate;
import edu.uci.ics.fuzzyjoin.spark.tokens.Scalar.ScalarReduceSelect;
import scala.Tuple2;

public class TokensBasic {
    public static void main(JavaRDD<String> records, JavaSparkContext sc) throws IOException {

        JavaPairRDD<String, Integer> tokensOne;
        JavaPairRDD<String, Integer> tokensCount;

        if (Main.TOKENS_PACKAGE_VALUE.equals("Array")) {
            LogUtil.logStage("Phase 1 : Map : Array");
            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE"); // TODO

            // tokensOne = records.flatMapToPair(new ArrayMap());
            // tokensCount = tokensOne.aggregateByKey(
            // new Integer[] { 0, Integer.MAX_VALUE, Integer.MIN_VALUE },
            // new ArrayReduceAggregate(),
            // new ArrayReduceAggregate());
            tokensOne = records.flatMapToPair(null);

            LogUtil.logStage("Phase 1 : Reduce : Array");
            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE");

            tokensCount = tokensOne;

        } else {
            LogUtil.logStage("Phase 1 : Map : Scalar");
            tokensOne = records.flatMapToPair(new ScalarMap());

            LogUtil.logStage("Phase 1 : Reduce : Scalar");
            tokensCount = tokensOne.reduceByKey(new ScalarReduceAggregate());
        }

        showPairRDD(tokensOne);
        LogUtil.logStage("");
        showPairRDD(tokensCount);

        JavaPairRDD<Integer, String> tokensCountInverted;
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, String>> tokensCountInvertedSorted;

        if (Main.TOKENS_PACKAGE_VALUE.equals("Array")) {
            LogUtil.logStage("Phase 2 : Map : Array");

            // tokensOne = records.flatMapToPair(new ArrayMap());
            // tokensCount = tokensOne.aggregateByKey(
            // new Integer[] { 0, Integer.MAX_VALUE, Integer.MIN_VALUE },
            // new ArrayReduceAggregate(),
            // new ArrayReduceAggregate());
            tokensCountInverted = tokensCount.mapToPair(new MapSelect());

            LogUtil.logStage("Phase 2 : Reduce : Array");
            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE"); // TODO

            tokensCountInvertedSorted = tokensCountInverted.mapToPair(t -> new Tuple2<>(t, t));

        } else {
            LogUtil.logStage("Phase 2 : Map : Scalar");
            tokensCountInverted = tokensCount.mapToPair(new MapSelect());

            LogUtil.logStage("Phase 2 : Reduce : Scalar");
            tokensCountInvertedSorted = tokensCountInverted.mapToPair(t -> new Tuple2<>(t, t))
                    .sortByKey(new ScalarReduceSelect());
        }

        showPairRDDInverted(tokensCountInverted.mapToPair(t -> t));
        LogUtil.logStage("");
        showPairRDDInverted(tokensCountInvertedSorted.mapToPair(t -> t._1));

    }

    private static void showPairRDD(JavaPairRDD<String, Integer> rdd) {
        List<Tuple2<String, Integer>> results = rdd.collect();
        results.forEach(r -> System.out.println("Id : " + r._1() + " Value : " + r._2()));
    }

    private static void showPairRDDInverted(JavaPairRDD<Integer, String> rdd) {
        List<Tuple2<Integer, String>> results = rdd.collect();
        results.forEach(r -> System.out.println("Value : " + r._1() + " Id : " + r._2()));
    }
}
