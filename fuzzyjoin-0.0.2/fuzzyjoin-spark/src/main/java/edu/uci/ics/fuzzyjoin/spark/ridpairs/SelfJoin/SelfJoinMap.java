package edu.uci.ics.fuzzyjoin.spark.ridpairs.SelfJoin;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroup;
import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroupFactory;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.fuzzyjoin.spark.Main;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.ridpairs.IntPair;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenRank;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenRankFrequency;

/**
 * @author rares
 * 
 *         KEY1: Unused
 * 
 *         VALUE1: Record
 * 
 *         KEY2: Token Group, Length
 * 
 *         VALUE2: RID, Tokens
 */
public class SelfJoinMap implements PairFlatMapFunction<String, IntPair, ValueSelfJoin> {

    private transient IntPair outputKey = new IntPair();
    private transient ValueSelfJoin outputValue = new ValueSelfJoin();
    private transient int[] dataColumns;
    private transient RecordGroup recordGroup;
    private transient SimilarityFilters similarityFilters;
    private transient Tokenizer tokenizer;
    private final TokenRank tokenRank = new TokenRankFrequency();

    // Constructor to initialize the necessary components
    /**
     * Constructor
     * 
     * @param sc:               JavaSparkContext to get the config parameters
     * @param tokenRankStrings: TokenRank strings from the RDD that is the result of
     *                          the Phase 1
     */
    public SelfJoinMap(JavaSparkContext sc, String[] tokenStrings) {

        //
        // set Tokenizer and SimilarityFilters
        //

        tokenizer = TokenizerFactory.getTokenizer(sc.getConf().get(
                FuzzyJoinConfig.TOKENIZER_PROPERTY,
                FuzzyJoinConfig.TOKENIZER_VALUE),
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);

        String similarityName = sc.getConf().get(
                FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_NAME_VALUE);
        float similarityThreshold = (float) sc.getConf().getDouble(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
        similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
                similarityName, similarityThreshold);

        //
        // set TokenRank and TokenGroup
        //

        // Set the tokenRanks from the tokenRankStrings which represents the ordered
        // tokens by frequency and alphabetically
        for (String tokenRankString : tokenStrings) {
            tokenRank.add(tokenRankString);
        }

        Path lengthstatsPath = Paths.get(sc.getConf().get(
                Main.DATA_LENGTHSTATS_PROPERTY)); // ALWAYS FALSE UNTIL FURTHER MORE COMPREHENSION

        String recordGroupClass = sc.getConf().get(
                Main.RIDPAIRS_GROUP_CLASS_PROPERTY,
                Main.RIDPAIRS_GROUP_CLASS_VALUE);
        int recordGroupFactor = sc.getConf().getInt(
                Main.RIDPAIRS_GROUP_FACTOR_PROPERTY,
                Main.RIDPAIRS_GROUP_FACTOR_VALUE);
        recordGroup = RecordGroupFactory.getRecordGroup(recordGroupClass,
                Math.max(1, sc.defaultParallelism() * recordGroupFactor),
                similarityFilters, "" + lengthstatsPath);

        //
        // set dataColumn
        //

        dataColumns = FuzzyJoinUtil.getDataColumns(sc.getConf().get(
                FuzzyJoinConfig.RECORD_DATA_VALUE,
                FuzzyJoinConfig.RECORD_DATA_VALUE));
    }

    // The call method to implement the flatMapToPair transformation
    @Override
    public Iterator<Tuple2<IntPair, ValueSelfJoin>> call(String inputValue)
            throws Exception {
        List<Tuple2<IntPair, ValueSelfJoin>> result = new ArrayList<>();

        //
        // get RID and Tokens
        //

        String splits[] = inputValue.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
        LogUtil.logStage("RID: " + splits[FuzzyJoinConfig.RECORD_KEY] + " for input value " + inputValue);

        int rid;
        try {
            System.out.println("RID: " + splits[FuzzyJoinConfig.RECORD_KEY] + " for input value " + inputValue);
            rid = Integer.parseInt(splits[FuzzyJoinConfig.RECORD_KEY]);
        } catch (NumberFormatException e) {
            // Handle the case where the RID is not a valid integer
            // For example, skip this record or log an error
            System.out.println("Invalid RID: " + splits[FuzzyJoinConfig.RECORD_KEY] + " for input value " + inputValue);
            return result.iterator(); // Skip this record
        }

        Collection<Integer> tokensRanked = tokenRank.getTokenRanks(
                tokenizer.tokenize(FuzzyJoinUtil.getData(splits, dataColumns,
                        FuzzyJoinConfig.TOKEN_SEPARATOR)));

        //
        // get Tokens as a DataBag and token groups
        //

        int length = tokensRanked.size();
        HashSet<Integer> tokenGroups = new HashSet<Integer>();
        int prefixLength = similarityFilters.getPrefixLength(length);
        int position = 0;
        if (recordGroup.isLengthOnly()) {
            for (Integer group : recordGroup.getGroups(0, length)) {
                tokenGroups.add(group);
            }
        }
        for (Integer token : tokensRanked) {
            if (!recordGroup.isLengthOnly()) {
                if (position < prefixLength) {
                    for (Integer group : recordGroup.getGroups(token, length)) {
                        tokenGroups.add(group);
                    }
                }
                position++;
            }
        }

        //
        // Key
        //

        outputKey.setSecond(length);

        //
        // Value
        //

        outputValue.setRID(rid);
        outputValue.setTokens(tokensRanked);

        //
        // output one pair per group
        //

        for (Integer group : tokenGroups) {

            //
            // Key
            //

            outputKey.setFirst(group);

            //
            // collect
            //

            result.add(new Tuple2<>(outputKey, outputValue));
        }

        return result.iterator();
    }
}