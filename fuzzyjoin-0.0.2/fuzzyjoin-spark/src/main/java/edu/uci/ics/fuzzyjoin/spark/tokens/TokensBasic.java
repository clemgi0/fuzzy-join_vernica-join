package edu.uci.ics.fuzzyjoin.spark.tokens;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
import scala.Tuple2;

public class TokensBasic {
    public static void main(JavaRDD<String> records, JavaSparkContext sc) throws IOException {

        System.out.println();
        System.out.println("-------------------- Phase 1 : Map --------------------");
        System.out.println();

        Tokenizer tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);
        int[] dataColumns = FuzzyJoinUtil.getDataColumns(FuzzyJoinConfig.RECORD_DATA_VALUE);

        JavaPairRDD<String, Integer> tokens = records.flatMapToPair(r -> {
            List<String> t = tokenizer
                    .tokenize(FuzzyJoinUtil.getData(r.toString().split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                            dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
            return t.stream().map(token -> new Tuple2<>(token, 1)).iterator();
        });

        showPairRDD(tokens);

    }

    private static void showRDD(JavaRDD<String> rdd) {
        List<String> results = rdd.collect();
        results.forEach(r -> System.out.println("Id : " + r));
    }

    private static void showPairRDD(JavaPairRDD<String, Integer> rdd) {
        List<Tuple2<String, Integer>> results = rdd.collect();
        results.forEach(r -> System.out.println("Id : " + r._1() + " Value : " + r._2()));
    }
}
