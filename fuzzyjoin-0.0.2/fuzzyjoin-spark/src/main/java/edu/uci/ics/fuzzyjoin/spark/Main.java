package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.spark.tokens.TokensBasic;

public class Main {
    public static final String NAMESPACE = "fuzzyjoin";
    public static final String VERSION_PROPERTY = NAMESPACE + ".version";
    // tokens package
    public static final String TOKENS_PACKAGE_PROPERTY = NAMESPACE + ".tokens.package";
    public static final String TOKENS_PACKAGE_VALUE = "Scalar";
    // tokens length stats
    public static final String TOKENS_LENGTHSTATS_PROPERTY = NAMESPACE + ".tokens.lengthstats";
    public static final boolean TOKENS_LENGTHSTATS_VALUE = false;
    // record group
    public static final String RIDPAIRS_GROUP_CLASS_PROPERTY = NAMESPACE + ".ridpairs.group.class";
    public static final String RIDPAIRS_GROUP_CLASS_VALUE = "TokenIdentity";
    public static final String RIDPAIRS_GROUP_FACTOR_PROPERTY = NAMESPACE + ".ridpairs.group.factor";
    public static final int RIDPAIRS_GROUP_FACTOR_VALUE = 1;
    // data properties
    public static final String DATA_DIR_PROPERTY = NAMESPACE + ".data.dir";
    public static final String DATA_RAW_PROPERTY = NAMESPACE + ".data.raw";
    public static final String DATA_RAW_VALUE = "*";
    public static final String DATA_LENGTHSTATS_PROPERTY = NAMESPACE + ".data.lengthstats";
    public static final String DATA_JOININDEX_PROPERTY = NAMESPACE + ".data.joinindex";
    public static final String DATA_CRTCOPY_PROPERTY = NAMESPACE + ".data.crtcopy";
    public static final String DATA_COPY_PROPERTY = NAMESPACE + ".data.copy";
    public static final String DATA_COPY_START_PROPERTY = NAMESPACE + ".data.copystart";
    public static final String DATA_SUFFIX_INPUT_PROPERTY = NAMESPACE + ".data.suffix.input";
    public static final String DATA_NORECORDS_PROPERTY = NAMESPACE + ".data.norecords";
    public static final String DATA_DICTIONARY_FACTOR_PROPERTY = NAMESPACE + ".data.dictionary.factor";
    // other constants
    public static final String DATA_LENGTH_STATS_FILE = "lengthstats";
    public static final char SEPSARATOR = ',';
    public static final String SEPSARATOR_REGEX = ",";

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        System.out.println("Hello, World!");

        SparkSession spark = SparkSession.builder()
                .appName("FuzzyJoinSpark")
                .master("yarn")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> records = spark.read().textFile(args[0]).javaRDD();

        //
        // ****************************** Print infos ******************************
        //

        String ret = "Main" + sc.appName() + "\n"
                + "  Input Path:  {";
        ret += "}\n";
        ret += "  Properties:  {";
        String[][] properties = new String[][] {
                new String[] { FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
                        FuzzyJoinConfig.SIMILARITY_NAME_VALUE },
                new String[] { FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                        "" + FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE },
                new String[] { FuzzyJoinConfig.TOKENIZER_PROPERTY, FuzzyJoinConfig.TOKENIZER_VALUE },
                new String[] { TOKENS_PACKAGE_PROPERTY, TOKENS_PACKAGE_VALUE },
                new String[] { TOKENS_LENGTHSTATS_PROPERTY, "" + TOKENS_LENGTHSTATS_VALUE },
                new String[] { RIDPAIRS_GROUP_CLASS_PROPERTY, RIDPAIRS_GROUP_CLASS_VALUE },
                new String[] { RIDPAIRS_GROUP_FACTOR_PROPERTY, "" + RIDPAIRS_GROUP_FACTOR_VALUE },
                new String[] { FuzzyJoinConfig.DATA_TOKENS_PROPERTY, "" },
                new String[] { DATA_JOININDEX_PROPERTY, "" }, };
        for (int crt = 0; crt < properties.length; crt++) {
            if (crt > 0) {
                ret += "\n                ";
            }
            ret += properties[crt][0];
        }
        ret += "}";
        System.out.println(ret);

        //
        // ****************************** Phase 1: Tokens ******************************
        //

        TokensBasic.main(records, sc);

        sc.close();
        spark.stop();
    }
}
