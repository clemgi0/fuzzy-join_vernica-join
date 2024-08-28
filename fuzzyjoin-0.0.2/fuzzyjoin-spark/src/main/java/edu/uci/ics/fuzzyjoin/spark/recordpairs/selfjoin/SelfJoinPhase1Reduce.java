package edu.uci.ics.fuzzyjoin.spark.recordpairs.selfjoin;

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
        IntPair outputKey = new IntPair();
        String outputValue;
        Integer rid = inputTuple._1;
        List<String> ridsSims = new ArrayList<>();
        String record = null;
        Set<String> uniqueStrings = new HashSet<>();
        HashSet<Integer> rids = new HashSet<Integer>();
        Collection<Tuple2<IntPair, String>> output = new ArrayList<Tuple2<IntPair, String>>();

        // Find the record and make unique each ridsSims
        Iterator<String> inputIte = inputTuple._2.iterator();
        while (inputIte.hasNext()) {
            String s = inputIte.next();
            if (!uniqueStrings.contains(s)) {
                uniqueStrings.add(s);
                if (s.contains(":") && record == null) {
                    record = s;
                } else {
                    ridsSims.add(s);
                }
            }
        }

        // If only record, return empty
        if (ridsSims.size() == 1) {
            return Collections.emptyList();
        }

        // Create the outputs like KEY: [RID1, RID2] and VALUE: "Sim;Record1"
        for (String value : ridsSims) {
            String valueSplit[] = value.split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);
            Integer rid2 = Integer.parseInt(valueSplit[0]);
            if (!rids.contains(rid2)) {
                rids.add(rid2);

                if (rid < rid2) {
                    outputKey.setFirst(rid);
                    outputKey.setSecond(rid2);
                } else {
                    outputKey.setFirst(rid2);
                    outputKey.setSecond(rid);
                }
                outputValue = valueSplit[1] + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + record;
                output.add(new Tuple2<IntPair, String>(outputKey, outputValue));
            }
        }
        rids.clear();

        return output;
    }

}
