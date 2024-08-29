package edu.uci.ics.fuzzyjoin.spark.stages.tokens.scalar;

import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * @param records RDD containing the tuple <Token, 1>
 *
 * @apiNote sort the pairs by key then by value
 *
 * @return JavaPairRDD<Integer, String> containing a token and it's sum for each
 *         token
 */

// UNUSED, LETS KEEP IT FOR NOW
public class ScalarReduceSelect
        implements Function2<Tuple2<Integer, String>, Tuple2<Integer, String>, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String call(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
        if (t1._1() > t2._1()) {
            return t2._2();
        } else if (t1._1() < t2._1()) {
            return t1._2();
        } else if (t1._2().compareTo(t2._2()) < 0) {
            return t1._2();
        } else {
            return t2._2();
        }
    }
}
