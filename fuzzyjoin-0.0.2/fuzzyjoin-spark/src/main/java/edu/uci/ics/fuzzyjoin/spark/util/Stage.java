package edu.uci.ics.fuzzyjoin.spark.util;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;

@FunctionalInterface
public interface Stage {
    void run(JavaSparkContext sc) throws IOException;
}