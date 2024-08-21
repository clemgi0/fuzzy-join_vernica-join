package edu.uci.ics.fuzzyjoin.spark.ridrecordpairs;

import java.io.DataInput;
import java.io.IOException;

public class IntTriple {
    private int first;

    private int second;

    private int third;

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public int getThird() {
        return third;
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        third = in.readInt();
    }

    public void set(int first, int second, int third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public void setThird(int third) {
        this.third = third;
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + "," + third + ")";
    }
}
