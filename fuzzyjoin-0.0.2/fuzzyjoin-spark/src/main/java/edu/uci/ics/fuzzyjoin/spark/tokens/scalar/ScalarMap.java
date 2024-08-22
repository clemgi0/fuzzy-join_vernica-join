package edu.uci.ics.fuzzyjoin.spark.tokens.scalar;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
import scala.Tuple2;

/**
 ** @param records RDD containing the records
 **
 ** 
 ** @apiNote get data values from the columns selected
 **          then tokenizes the data using te TOKEN_SEPARATOR
 **          and then maps each token with a 1
 **
 ** @return JavaPairRDD<String, Integer> containing the tokens and a 1 for each
 *         token
 */
public class ScalarMap implements PairFlatMapFunction<String, String, Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Tuple2<String, Integer>> call(String record) {
        Tokenizer tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);
        int[] dataColumns = FuzzyJoinUtil.getDataColumns(FuzzyJoinConfig.RECORD_DATA_VALUE);

        List<String> tokens = tokenizer.tokenize(
                FuzzyJoinUtil.getData(record.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                        dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));

        return tokens.stream().map(token -> new Tuple2<>(token, 1)).collect(Collectors.toList()).iterator();
    }
}