package edu.uci.ics.fuzzyjoin.spark.tokens.array;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.Function2;

// pas tester, abandonner pour le moment
public class ArrayReduceAggregate implements Function2<Integer[], Integer[], Integer[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public Integer[] call(Integer[] token1, Integer[] token2)
            throws Exception {
        int count1 = token1[0];
        int count2 = token2[0];
        int length1 = token1.length > 1 ? token1[1] : 0;
        int length2 = token2.length > 1 ? token2[1] : 0;

        int count = count1 + count2;
        int min = Math.min(length1, length2);
        int max = Math.max(length1, length2);

        HashMap<Integer, Integer> lengthFreq = new HashMap<>();
        if (length1 > 0) {
            lengthFreq.put(length1, lengthFreq.getOrDefault(length1, 0) + 1);
        }
        if (length2 > 0) {
            lengthFreq.put(length2, lengthFreq.getOrDefault(length2, 0) + 1);
        }

        Integer[] result = new Integer[3];
        result[0] = count;
        result[1] = min;
        result[2] = max;

        // Adding length frequencies if needed
        if (!lengthFreq.isEmpty()) {
            result = new Integer[3 + lengthFreq.size() * 2];
            result[0] = count;
            result[1] = min;
            result[2] = max;
            int i = 3;
            for (Map.Entry<Integer, Integer> entry : lengthFreq.entrySet()) {
                result[i++] = entry.getKey();
                result[i++] = entry.getValue();
            }
        }

        return result;
    }
}
