package edu.uci.ics.fuzzyjoin.spark.recordpairs;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.spark.Main;

public class ReducePhase2 implements Function2<String, String, String> {
    private final boolean isSelfJoin;

    public ReducePhase2(JavaSparkContext sc) {
        isSelfJoin = "".equals(sc.getConf().get(Main.DATA_SUFFIX_INPUT_PROPERTY, ""));
    }

    @Override
    public String call(String record1, String record2) throws Exception {
        String records[] = new String[2];
        String splits[] = record1.split(FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR_REGEX);
        String sim = splits[0];
        records[0] = splits[1];

        int rid1, rid2;
        rid1 = Integer.valueOf(records[0].split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX)[FuzzyJoinConfig.RECORD_KEY]);

        records[1] = record2.split(FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR_REGEX)[1];
        rid2 = Integer
                .valueOf(records[1]
                        .split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX)[FuzzyJoinConfig.RECORD_KEY]);

        // if ((isSelfJoin && rid1 != rid2)
        // || (!isSelfJoin && !records[0].equals(records[1]))) {
        // /*
        // * if you want to have correct job counters (i.e. map output
        // * records = reduce input records)
        // */
        // // while (values.hasNext()) {
        // // values.next();
        // // }
        // break;
        // }

        int i0 = 0, i1 = 1;
        if (isSelfJoin && rid1 > rid2) {
            i0 = 1;
            i1 = 0;
        }

        return records[i0] + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + sim
                + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR
                + records[i1];
    }
}
