package edu.uci.ics.fuzzyjoin.spark.tokens;

import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;

public class TokensBasic {
    public static void main(JavaRDD<String> records, JavaSparkContext sc) throws IOException {
        //
        // Phase 1: Tokenize records
        //

        Tokenizer tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);

        int[] dataColumns = FuzzyJoinUtil.getDataColumns(FuzzyJoinConfig.RECORD_DATA_VALUE);

        JavaRDD<String> tokens = records.flatMap(r -> {
            List<String> t = tokenizer
                    .tokenize(FuzzyJoinUtil.getData(r.toString().split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                            dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
            return t.iterator();
        });

        List<String> results = tokens.collect();

        results.forEach(r -> System.out.println("Id : " + r));
    }
}
