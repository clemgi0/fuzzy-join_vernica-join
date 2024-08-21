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

    private int[] dataColumns;
    private RecordGroup recordGroup;
    private SimilarityFilters similarityFilters;
    private Tokenizer tokenizer;
    private final TokenRank tokenRank = new TokenRankFrequency();
    private String lengthstatsPathValue;
    private Path lengthstatsPath;
    private String recordGroupClass;
    private int recordGroupFactor;
    private int numReduce;

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
        // set TokenRank and params for TokenGroup
        //

        // Set the tokenRanks from the tokenRankStrings which represents the ordered
        // tokens by frequency and alphabetically
        for (String tokenRankString : tokenStrings) {
            tokenRank.add(tokenRankString);
        }

        lengthstatsPathValue = sc.getConf().get(
                Main.DATA_LENGTHSTATS_PROPERTY); // ALWAYS FALSE UNTIL FURTHER MORE COMPREHENSION

        recordGroupClass = sc.getConf().get(
                Main.RIDPAIRS_GROUP_CLASS_PROPERTY,
                Main.RIDPAIRS_GROUP_CLASS_VALUE);
        recordGroupFactor = sc.getConf().getInt(
                Main.RIDPAIRS_GROUP_FACTOR_PROPERTY,
                Main.RIDPAIRS_GROUP_FACTOR_VALUE);

        numReduce = sc.defaultParallelism();

        //
        // set dataColumn
        //

        dataColumns = FuzzyJoinUtil.getDataColumns(sc.getConf().get(
                FuzzyJoinConfig.RECORD_DATA_PROPERTY,
                FuzzyJoinConfig.RECORD_DATA_VALUE).toString());
    }

    // The call method to implement the flatMapToPair transformation
    @Override
    public Iterator<Tuple2<IntPair, ValueSelfJoin>> call(String inputValue)
            throws Exception {
        List<Tuple2<IntPair, ValueSelfJoin>> result = new ArrayList<Tuple2<IntPair, ValueSelfJoin>>();

        //
        // set TokenGroup
        //

        lengthstatsPath = Paths.get(lengthstatsPathValue);

        recordGroup = RecordGroupFactory.getRecordGroup(recordGroupClass,
                Math.max(1, numReduce * recordGroupFactor),
                similarityFilters, "" + lengthstatsPath);

        //
        // get RID and Tokens
        //

        String splits[] = inputValue.split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
        int rid = Integer.parseInt(splits[FuzzyJoinConfig.RECORD_KEY]);
        Collection<Integer> tokensRanked = tokenRank
                .getTokenRanks(tokenizer
                        .tokenize(FuzzyJoinUtil.getData(splits, dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR)));

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
        // Value
        //

        ValueSelfJoin outputValue = new ValueSelfJoin(rid, tokensRanked);

        //
        // output one pair per group
        //

        for (Integer group : tokenGroups) {

            //
            // Key
            //

            IntPair outputKey = new IntPair(group, length);

            //
            // collect
            //

            result.add(new Tuple2<>(outputKey, outputValue));
        }

        //
        // Debugging
        //

        // System.out
        // .println("Splits : " + Arrays.toString(splits) + " for input value " +
        // inputValue + " with datacolumns "
        // + Arrays.toString(dataColumns) + " and token separator " +
        // FuzzyJoinConfig.TOKEN_SEPARATOR
        // + " and token ranks " + tokenRank.getRank(splits[2] + " | " +
        // tokenRank.getRank(inputValue)));
        // for (Tuple2<IntPair, ValueSelfJoin> res : result) {
        // System.out.println("Result Key[0] : " + res._1().getFirst() + " | Key[1] : "
        // + res._1().getSecond() + " || Value[0] : "
        // + res._2().getRID() + " Value[1] : " +
        // Arrays.toString(res._2().getTokens()));
        // }
        // System.out.println();

        return result.iterator();
    }
}