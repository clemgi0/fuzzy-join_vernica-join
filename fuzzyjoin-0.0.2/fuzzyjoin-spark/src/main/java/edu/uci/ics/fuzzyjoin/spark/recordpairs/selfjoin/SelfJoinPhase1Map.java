package edu.uci.ics.fuzzyjoin.spark.recordpairs.selfjoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import scala.Tuple2;

public class SelfJoinPhase1Map implements PairFlatMapFunction<String, String, Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public Iterator<Tuple2<String, Integer>> call(String inputValue) throws Exception {
        Integer outputValue;
        String outputKey;

        if (inputValue.indexOf(FuzzyJoinConfig.RECORD_SEPARATOR) != -1) {
            /*
             * VALUE1: RID:Record
             *
             * KEY2: Record
             *
             * VALUE2: Rid
             */
            String valueSplit[] = inputValue.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
            outputValue = Integer.valueOf(valueSplit[FuzzyJoinConfig.RECORD_KEY]);
            outputKey = inputValue;

            return Collections.singletonList(new Tuple2<>(outputKey,
                    outputValue)).iterator();
        } else {
            /*
             * VALUE1: "RID1 RID2 Similarity"
             *
             * KEY2: "RID2 Similarity" and "RID1 Similarity"
             *
             * VALUE2: RID1 and RID2
             */
            String valueSplit[] = inputValue.split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);

            // set first result to RID1
            outputValue = Integer.parseInt(valueSplit[0]);
            outputKey = valueSplit[1] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR +
                    valueSplit[2];
            Tuple2<String, Integer> result1 = new Tuple2<>(outputKey, outputValue);

            // set second result to RID2
            outputValue = Integer.parseInt(valueSplit[1]);
            outputKey = valueSplit[0] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR +
                    valueSplit[2];
            Tuple2<String, Integer> result2 = new Tuple2<>(outputKey, outputValue);

            // return both results
            ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
            result.add(result1);
            result.add(result2);
            return result.iterator();
        }
    }
}
