package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.logging.RedirectOutput;
import edu.uci.ics.fuzzyjoin.spark.starters.StartFuzzyJoin;
import edu.uci.ics.fuzzyjoin.spark.starters.StartRidPairsPPJoin;
import edu.uci.ics.fuzzyjoin.spark.starters.StartTokensBasic;

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
    public static final char SEPARATOR = ',';
    public static final String SEPARATOR_REGEX = ",";

    public static void main(String[] args) throws IOException {
        // conf variables
        SparkConfig configuration;
        SparkConf sparkConf;
        JavaSparkContext sc;

        //
        // Handling the args
        //

        if (args.length < 1) {
            System.err.println("Usage: spark-submit \\\n" +
                    "--class PATH_TO_MAIN \\\n" +
                    "--master YARN \\\n" +
                    "PATH_TO_JAR \\\n" +
                    "NAME_OF_CONFIG_FILE \\\n" +
                    "STAGES_TO_RUN \\\n" +
                    "[OPTIONS] : \\\n" +
                    "LOG_TO_FILE: true/false");
            System.exit(1);
        }

        // Set config file
        configuration = new SparkConfig();
        sparkConf = configuration.getSparkContext(args[1]);

        if (args.length > 0) {
            configuration.readConfig("fuzzyjoin/", args[0]);
        } else {
            LogUtil.logStage("Configuration file not specified, using default dblp.quickstart.xml");
            configuration.readConfig("fuzzyjoin/", "dblp.quickstart.xml");
        }

        // Redirect log output to file if specified
        if (args.length > 2 && args[2].equals("true")) {
            RedirectOutput.setFile("output.txt");
        } else {
            LogUtil.logStage("Log output to console");
        }

        // Print properties
        // configuration.printMainProperties();
        configuration.printAllProperties();

        //
        // Create Java Spark Context
        //

        LogUtil.logStage("Creating Java Spark Context");
        sc = new JavaSparkContext(sparkConf);

        LogUtil.logStage("Starting of the app");

        //
        // Select stages to run
        //

        switch (args[1].toLowerCase()) {
            case "tokensbasic":
                StartTokensBasic.start(sc, true);
                break;

            case "ridpairsppjoin":
                StartRidPairsPPJoin.start(sc, true, null);
                break;

            case "fuzzyjoin":
                StartFuzzyJoin.start(sc);
                break;

            default:
                LogUtil.logStage("Please select a correct stage between:\n" +
                        "-TokensBasic\n" +
                        "-RidPairsPPJoin\n" +
                        "-RecordPairsBasic (work in progress)\n" +
                        "-FuzzyJoin");
                break;
        }

        //
        // Ending of the app
        //

        LogUtil.logStage("Close Java Spark Context and Spark Session");
        sc.close();

        LogUtil.logStage("Ending of the app");
        RedirectOutput.setConsole();
    }
}
