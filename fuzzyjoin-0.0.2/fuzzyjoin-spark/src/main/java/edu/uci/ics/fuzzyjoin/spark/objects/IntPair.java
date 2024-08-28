package edu.uci.ics.fuzzyjoin.spark.objects;

import java.io.Serializable;

public class IntPair implements Serializable, Comparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int compareTo(IntPair other) {
        if (this.first != other.first) {
            return Integer.compare(this.first, other.first);
        }
        return Integer.compare(this.second, other.second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IntPair intPair = (IntPair) o;
        return first == intPair.first && second == intPair.second;
    }

    @Override
    public int hashCode() {
        return 31 * first + second;
    }

    @Override
    public String toString() {
        return "IntPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
