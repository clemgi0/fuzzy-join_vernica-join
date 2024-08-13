// package edu.uci.ics.fuzzyjoin.spark.ridpairs;

// import java.io.IOException;

// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;

// import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
// import edu.uci.ics.fuzzyjoin.spark.Main;
// import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
// import edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin.SelfJoinMap;
// import edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin.ValueSelfJoin;
// import scala.Tuple2;

// public class RIDPairsPPJoin {
// public static void main(JavaPairRDD<Integer, String> tokendsRank,
// JavaRDD<String> records, JavaSparkContext sc)
// throws IOException {

// JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer[]>>
// tokensGrouped;
// if (Main.DATA_SUFFIX_INPUT_PROPERTY.isEmpty()) {
// // self-join

// LogUtil.logStage("Self-join : Map");
// JavaPairRDD<IntPair, ValueSelfJoin> = records.flatMapToPair(new
// SelfJoinMap());

// LogUtil.logStage("Self-join : Reduce");
// // job.setReducerClass(ReduceSelfJoin

// } else {
// // R-S join

// System.out.println("PAS COMPLETEMENT IMPLEMENTE, A FINIR PEUT ETRE");
// LogUtil.logStage("R-S join : Map");
// // job.setMapperClass(MapJoin

// LogUtil.logStage("R-S join : Reduce");
// // job.setReducerClass(ReduceJoin
// }

// }
// }
