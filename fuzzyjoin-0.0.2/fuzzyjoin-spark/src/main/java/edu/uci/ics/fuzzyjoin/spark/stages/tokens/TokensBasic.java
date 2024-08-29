package edu.uci.ics.fuzzyjoin.spark.stages.tokens;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.stages.tokens.scalar.ScalarPhase1Map;
import edu.uci.ics.fuzzyjoin.spark.stages.tokens.scalar.ScalarPhase1Reduce;
import edu.uci.ics.fuzzyjoin.spark.util.LogUtil;
import scala.Tuple2;

public class TokensBasic {
    public static JavaRDD<String> main(JavaSparkContext sc, JavaRDD<String> records) {
        //
        // -------------------- PHASE 1 --------------------
        //

        JavaPairRDD<String, Integer> tokensOne;
        JavaPairRDD<String, Integer> tokensCount;

        if (sc.getConf().get(Main.TOKENS_PACKAGE_PROPERTY).equals("Array")) {
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
            tokensOne = records.flatMapToPair(new ScalarPhase1Map(sc));

            LogUtil.logStage("Phase 1 : Reduce : Scalar");
            tokensCount = tokensOne.reduceByKey(new ScalarPhase1Reduce());
        }

        //
        // -------------------- PHASE 2 --------------------
        //

        JavaPairRDD<Integer, String> tokensCountInverted;
        JavaRDD<String> tokensCountInvertedSorted;

        if (sc.getConf().get(Main.TOKENS_PACKAGE_PROPERTY).equals("Array")) {
            LogUtil.logStage("Phase 2 : Map : Array");

            // tokensOne = records.flatMapToPair(new ArrayMap());
            // tokensCount = tokensOne.aggregateByKey(
            // new Integer[] { 0, Integer.MAX_VALUE, Integer.MIN_VALUE },
            // new ArrayReduceAggregate(),
            // new ArrayReduceAggregate());
            tokensCountInverted = tokensCount.mapToPair(new Phase2Map());

            LogUtil.logStage("Phase 2 : Reduce : Array");
            System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE"); // TODO

            tokensCountInvertedSorted = tokensCountInverted.map(t -> t._2);

        } else {
            LogUtil.logStage("Phase 2 : Map : Scalar");
            tokensCountInverted = tokensCount.mapToPair(new Phase2Map());

            // showPairRDDInverted(tokensCountInverted);

            // LogUtil.logStage("Phase 2 : Reduce : Scalar");

            // Step 1: Sort by key (ascending)
            JavaPairRDD<Integer, String> sortedByKey = tokensCountInverted.sortByKey();

            // Step 2: Sort by value (ascending)
            JavaPairRDD<String, Integer> sortedByKeyThenValue = sortedByKey
                    .mapToPair(Tuple2::swap) // Swap to have String as key for sorting
                    .sortByKey(); // Sort by value, which is now the key

            // Step 3: Extract the values (JavaRDD<String>)
            tokensCountInvertedSorted = sortedByKeyThenValue.map(Tuple2::_1);

        }

        // showRDDInverted(tokensCountInvertedSorted);

        // return the list of tokens ranked
        return tokensCountInvertedSorted;
    }

    private static void showPairRDD(JavaPairRDD<String, Integer> rdd) {
        List<Tuple2<String, Integer>> results = rdd.collect();
        results.forEach(r -> System.out.println("Value : " + r._1() + " Id : " + r._2()));
    }

    private static void showPairRDDInverted(JavaPairRDD<Integer, String> rdd) {
        List<Tuple2<Integer, String>> results = rdd.collect();
        results.forEach(r -> System.out.println("Value : " + r._1() + " Id : " + r._2()));
    }

    private static void showRDDInverted(JavaRDD<String> rdd) {
        List<String> results = rdd.collect();
        results.forEach(r -> System.out.println("" + r));
    }
}
