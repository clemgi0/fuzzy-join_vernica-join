package edu.uci.ics.fuzzyjoin.spark.stages.recordpairs.selfjoin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.spark.objects.IntPair;
import scala.Tuple2;

public class SelfJoinPhase1Reduce {

    public Collection<Tuple2<IntPair, String>> call(Tuple2<Integer, Iterable<String>> inputTuple) {
        Integer rid1 = inputTuple._1;
        List<String> ridsSims = new ArrayList<>();
        String record = null;
        Set<String> uniqueStrings = new HashSet<>();
        Collection<Tuple2<IntPair, String>> output = new ArrayList<Tuple2<IntPair, String>>();

        // Find the record and make unique each ridsSims
        Iterator<String> inputIte = inputTuple._2.iterator();
        while (inputIte.hasNext()) {
            String s = inputIte.next();
            System.out.println("\tString from input : " + s);
            if (!uniqueStrings.contains(s)) {
                uniqueStrings.add(s);
                if (s.contains(":") && record == null) {
                    record = s;
                } else {
                    ridsSims.add(s);
                }
            }
        }

        // System.out.println("ridsSims : " + ridsSims);
        // If only record, return empty
        if (ridsSims.size() == 0) {
            // System.out.println("Only record : " + record);
            return Collections.emptyList();
        }

        // Create the outputs like KEY: [RID1, RID2] and VALUE: "Sim;Record1"
        for (String value : ridsSims) {
            IntPair outputKey = new IntPair();

            String valueSplit[] = value.split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);
            Integer rid2 = Integer.parseInt(valueSplit[0]);

            if (rid1 < rid2) {
                outputKey.setFirst(rid1);
                outputKey.setSecond(rid2);
            } else {
                outputKey.setFirst(rid2);
                outputKey.setSecond(rid1);
            }

            String outputValue = valueSplit[1] + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + record;
            output.add(new Tuple2<IntPair, String>(outputKey, outputValue));
        }

        // System.out.println("Not only record : " + record);
        // output.forEach(r -> System.out.println("\t" + r._1().getFirst() + " " +
        // r._1().getSecond() + " " + r._2()));

        return output;
    }

}
