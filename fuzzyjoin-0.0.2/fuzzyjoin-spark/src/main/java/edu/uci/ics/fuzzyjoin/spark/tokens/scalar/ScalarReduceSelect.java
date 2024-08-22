package edu.uci.ics.fuzzyjoin.spark.tokens.scalar;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

/**
 * @param records RDD containing the tuple <Token, 1>
 *
 * @apiNote sort the pairs by key then by value
 *
 * @return JavaPairRDD<Integer, String> containing a token and it's sum for each
 *         token
 */
public class ScalarReduceSelect implements Comparator<Tuple2<Integer, String>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
        int keyComparison = t1._1().compareTo(t2._1());
        if (keyComparison != 0) {
            return keyComparison;
        } else {
            return t1._2().compareTo(t2._2());
        }
    }
}
