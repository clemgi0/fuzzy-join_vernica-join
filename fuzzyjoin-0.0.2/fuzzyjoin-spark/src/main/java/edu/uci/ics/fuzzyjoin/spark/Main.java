package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
            System.err.println("Usage: Main <file>");
            System.exit(1);
        }

        System.out.println();
        System.out.println("-------------------- Starting of the app --------------------");
        System.out.println();

        SparkConf conf = new SparkConf().setAppName("FuzzyJoinSpark");

        System.out.println();
        System.out.println("-------------------- Creating Java Spark Context --------------------");
        System.out.println();

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println();
        System.out.println("-------------------- Read files from HDFS --------------------");
        System.out.println();

        JavaRDD<String> records = sc.textFile(args[0]);

        System.out.println();
        System.out.println("-------------------- Print infos --------------------");
        System.out.println();

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

        System.out.println();
        System.out.println("-------------------- Start Stage 1 : TokensBasic --------------------");
        System.out.println();

        TokensBasic.main(records, sc);

        System.out.println();
        System.out.println("-------------------- Close Java Spark Context and Spark Session --------------------");
        System.out.println();

        sc.close();

        System.out.println();
        System.out.println("-------------------- Ending of the app --------------------");
        System.out.println();
    }
}
