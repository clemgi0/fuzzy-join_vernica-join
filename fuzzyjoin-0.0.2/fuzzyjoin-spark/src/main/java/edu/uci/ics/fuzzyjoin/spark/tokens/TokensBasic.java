package edu.uci.ics.fuzzyjoin.spark.tokens;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.tokens.Array.ArrayMap;
import edu.uci.ics.fuzzyjoin.spark.tokens.Scalar.ScalarMap;
import scala.Tuple2;

public class TokensBasic {
    public static void main(JavaRDD<String> records, JavaSparkContext sc) throws IOException {

        System.out.println();
        System.out.println("-------------------- Phase 1 : Map : ");

        JavaPairRDD<String, Integer> tokensOne;
        JavaPairRDD<String, Integer> tokensCount;

        if (Main.TOKENS_PACKAGE_VALUE.equals("Array")) {
            System.out.println("Array --------------------");
            System.out.println();

            tokensOne = records.flatMapToPair(new ArrayMap());
            tokensCount = tokensOne.reduceByKey(new ArrayReduceAggegate());
        } else {
            System.out.println("Scalar --------------------");
            System.out.println();

            tokensOne = new ScalarMap().map(records);
        }

        showPairRDD(tokensOne);

    }

    @SuppressWarnings("unused")
    private static void showRDD(JavaRDD<String> rdd) {
        List<String> results = rdd.collect();
        results.forEach(r -> System.out.println("Id : " + r));
    }

    private static void showPairRDD(JavaPairRDD<String, Integer> rdd) {
        List<Tuple2<String, Integer>> results = rdd.collect();
        results.forEach(r -> System.out.println("Id : " + r._1() + " Value : " + r._2()));
    }
}
