// package edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin;

// import org.apache.spark.api.java.function.PairFlatMapFunction;
// import scala.Tuple2;

// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.*;

// import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
// import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
// import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroup;
// import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroupFactory;
// import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
// import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
// import edu.uci.ics.fuzzyjoin.spark.ridpairs.IntPair;
// import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
// import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
// import edu.uci.ics.fuzzyjoin.tokenorder.TokenLoad;
// import edu.uci.ics.fuzzyjoin.tokenorder.TokenRank;
// import edu.uci.ics.fuzzyjoin.tokenorder.TokenRankFrequency;

// public class SelfJoinMap implements PairFlatMapFunction<String, IntPair,
// ValueSelfJoin> {
// private int[] dataColumns;
// private final TokenRank tokenRank = new TokenRankFrequency();
// private RecordGroup recordGroup;
// private SimilarityFilters similarityFilters;
// private Tokenizer tokenizer;

// // Constructor to initialize the necessary components
// public SelfJoinMap() {
// //
// // set Tokenizer and SimilarityFilters
// //
// tokenizer = TokenizerFactory.getTokenizer(FuzzyJoinConfig.TOKENIZER_VALUE,
// FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
// FuzzyJoinConfig.TOKEN_SEPARATOR);
// similarityFilters =
// SimilarityFiltersFactory.getSimilarityFilters(FuzzyJoinConfig.SIMILARITY_NAME_VALUE,
// FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
// //
// // set TokenRank and TokenGroup
// //
// Path tokensPath = Paths.get(FuzzyJoinConfig.DATA_TOKENS_PROPERTY);
// Path lengthstatsPath = lengthstatsPathStr != null ?
// Paths.get(FuzzyJoinDriver.DATA_LENGTHSTATS_PROPERTY) : null;
// TokenLoad tokenLoad = new TokenLoad(tokensPath.toString(), tokenRank);
// tokenLoad.loadTokenRank();
// recordGroup =
// RecordGroupFactory.getRecordGroup(FuzzyJoinDriver.RIDPAIRS_GROUP_CLASS_VALUE,
// Math.max(1, numReduceTasks * FuzzyJoinDriver.RIDPAIRS_GROUP_FACTOR_VALUE),
// similarityFilters,
// lengthstatsPath != null ? lengthstatsPath.toString() : "");
// //
// // set dataColumn
// //
// dataColumns =
// FuzzyJoinUtil.getDataColumns(FuzzyJoinConfig.RECORD_DATA_VALUE);
// }

// // The call method to implement the flatMapToPair transformation
// @Override
// public Iterator<Tuple2<IntPair, ValueSelfJoin>> call(String inputValue)
// throws Exception {
// List<Tuple2<IntPair, ValueSelfJoin>> result = new ArrayList<>();

// //
// // get RID and Tokens
// //
// String splits[] = inputValue.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
// int rid = Integer.parseInt(splits[FuzzyJoinConfig.RECORD_KEY]);
// Collection<Integer> tokensRanked = tokenRank.getTokenRanks(
// tokenizer.tokenize(FuzzyJoinUtil.getData(splits, dataColumns,
// FuzzyJoinConfig.TOKEN_SEPARATOR)));

// //
// // get Tokens as a DataBag and token groups
// //
// int length = tokensRanked.size();
// Set<Integer> tokenGroups = new HashSet<>();
// int prefixLength = similarityFilters.getPrefixLength(length);
// int position = 0;
// if (recordGroup.isLengthOnly()) {
// for (Integer group : recordGroup.getGroups(0, length)) {
// tokenGroups.add(group);
// }
// }
// for (Integer token : tokensRanked) {
// if (!recordGroup.isLengthOnly()) {
// if (position < prefixLength) {
// for (Integer group : recordGroup.getGroups(token, length)) {
// tokenGroups.add(group);
// }
// }
// position++;
// }
// }

// //
// // Key
// //
// IntPair outputKey = new IntPair();
// outputKey.setSecond(length);

// //
// // Value
// //
// ValueSelfJoin outputValue = new ValueSelfJoin();
// outputValue.setRID(rid);
// outputValue.setTokens(tokensRanked);

// //
// // output one pair per group
// //
// for (Integer group : tokenGroups) {
// //
// // Key
// //
// outputKey.setFirst(group);
// //
// // collect
// //
// result.add(new Tuple2<>(outputKey, outputValue));
// }

// return result.iterator();
// }
// }