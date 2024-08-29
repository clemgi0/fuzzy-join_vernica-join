package edu.uci.ics.fuzzyjoin.spark.stages.tokens;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Phase2Map implements PairFunction<Tuple2<String, Integer>, Integer, String> {

    @Override
    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
        return new Tuple2<Integer, String>(t._2(), t._1());
    }

}
