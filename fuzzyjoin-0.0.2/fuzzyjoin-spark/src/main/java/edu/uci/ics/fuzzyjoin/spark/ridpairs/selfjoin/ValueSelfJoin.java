package edu.uci.ics.fuzzyjoin.spark.ridpairs.selfjoin;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

public class ValueSelfJoin implements Serializable {
    private int rid;
    private int[] tokens;

    public ValueSelfJoin() {
    }

    public ValueSelfJoin(int rid, Collection<Integer> tokens) {
        this.rid = rid;
        setTokens(tokens);
    }

    public ValueSelfJoin(ValueSelfJoin v) {
        this.rid = v.rid;
        this.tokens = v.tokens;
    }

    public int getRID() {
        return rid;
    }

    public void setRID(int rid) {
        this.rid = rid;
    }

    public int[] getTokens() {
        return tokens;
    }

    public void setTokens(Collection<Integer> tokens) {
        this.tokens = tokens.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public String toString() {
        return rid + ":" + Arrays.toString(tokens);
    }
}
