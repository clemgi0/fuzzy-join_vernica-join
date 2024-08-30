package edu.uci.ics.fuzzyjoin.spark.starters;

import java.io.IOException;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.SparkConfig;
import edu.uci.ics.fuzzyjoin.spark.stages.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.spark.util.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.util.SaveResult;

public class StartRidPairsPPJoin {
    public static void start(JavaSparkContext sc) throws IOException {
        //
        // Read records from HDFS
        //
        SparkConfig configuration = new SparkConfig();

        LogUtil.logStage("Read records from HDFS");
        JavaRDD<String> records = configuration.readData(sc, "records");

        //
        // Read tokensRank from HDFS
        //
        LogUtil.logStage("Read tokens from HDFS");
        JavaRDD<String> tokensRankRDD = configuration.readData(sc, "tokens");
        String[] tokensRank = tokensRankRDD.collect().toArray(new String[0]);

        //
        // Launch Stage 2 : FuzzyJoin
        //
        LogUtil.logStage("Start Stage 2 : RIDPairsPPJoin");
        Date startTime = new Date();

        JavaRDD<String> ridPairs = RIDPairsPPJoin.main(tokensRank, records, sc);

        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "ridpairs");

        SaveResult saver = new SaveResult(sc, "ridpairs");
        saver.saveJavaStringRDD(ridPairs);
    }

    public static JavaRDD<String> start(JavaSparkContext sc, JavaRDD<String> records, String[] tokensRank)
            throws IOException {
        //
        // Launch Stage 2 : FuzzyJoin
        //
        LogUtil.logStage("Start Stage 2 : RIDPairsPPJoin");
        Date startTime = new Date();

        JavaRDD<String> ridPairs = RIDPairsPPJoin.main(tokensRank, records, sc);

        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "ridpairs");

        return ridPairs;
    }
}
