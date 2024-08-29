package edu.uci.ics.fuzzyjoin.spark.stages.tokens.scalar;

import org.apache.spark.api.java.function.Function2;

/**
 * @param records RDD containing the tuple <Token, 1>
 *
 * 
 * @apiNote compute the sum of the values for each token
 *
 * @return JavaPairRDD<String, Integer> containing the tokens and the sum for
 *         each token
 */
public class ScalarPhase1Reduce implements Function2<Integer, Integer, Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public Integer call(Integer value1, Integer value2) {
        return value1 + value2;
    }
}