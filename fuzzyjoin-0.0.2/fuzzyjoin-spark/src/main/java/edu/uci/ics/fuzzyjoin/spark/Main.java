package edu.uci.ics.fuzzyjoin.spark;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.uci.ics.fuzzyjoin.spark.starters.StartFuzzyJoin;
import edu.uci.ics.fuzzyjoin.spark.starters.StartRecordPairsBasic;
import edu.uci.ics.fuzzyjoin.spark.starters.StartRidPairsPPJoin;
import edu.uci.ics.fuzzyjoin.spark.starters.StartTokensBasic;
import edu.uci.ics.fuzzyjoin.spark.util.ArgsHandler;
import edu.uci.ics.fuzzyjoin.spark.util.LogUtil;
import edu.uci.ics.fuzzyjoin.spark.util.RedirectOutput;
import edu.uci.ics.fuzzyjoin.spark.util.Stage;

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
    // available stages
    public static final Map<String, Stage> STAGES = new HashMap<>();

    static {
        STAGES.put("tokensbasic", (sc) -> {
            StartTokensBasic.start(sc);
        });
        STAGES.put("ridpairsppjoin", (sc) -> {
            StartRidPairsPPJoin.start(sc);
        });
        STAGES.put("recordpairsbasic", (sc) -> {
            StartRecordPairsBasic.start(sc);
        });
        STAGES.put("fuzzyjoin", (sc) -> {
            StartFuzzyJoin.start(sc);
        });
    }

    public static void main(String[] args) throws IOException {
        //
        // Handle command-line arguments
        //
        ArgsHandler.handleArg(args);
        String configFile = ArgsHandler.getConfigFile(args);
        String stageName = ArgsHandler.getStage(args);
        boolean logToFile = ArgsHandler.shouldLogToFile(args);

        //
        // Create configuration variables
        //
        SparkConfig configuration;
        SparkConf sparkConf;
        JavaSparkContext sc;

        //
        // Set configuration file
        //
        configuration = new SparkConfig();
        sparkConf = configuration.getSparkContext(stageName);
        configuration.readConfig("fuzzyjoin/", configFile);

        //
        // Redirect log output to file if specified
        //
        if (logToFile) {
            RedirectOutput.setFile("output.txt");
        } else {
            LogUtil.logStage("Log output to console");
        }

        //
        // Print properties
        //
        // configuration.printMainProperties();
        configuration.printAllProperties();

        //
        // Create Java Spark Context
        //
        LogUtil.logStage("Creating Java Spark Context");
        Date startTime = new Date();
        sc = new JavaSparkContext(sparkConf);
        Date endTime = new Date();
        LogUtil.logTime(startTime, endTime, "Create JavaSparkContext");

        //
        // Run stage(s)
        //
        LogUtil.logStage("Starting of the app");
        Stage stage = STAGES.get(stageName);
        startTime = new Date();
        stage.run(sc);
        endTime = new Date();
        LogUtil.logTime(startTime, endTime, "App");

        //
        // Ending of the app
        //
        LogUtil.logStage("Close Java Spark Context and Spark Session");
        sc.close();
        LogUtil.logStage("Ending of the app");
        RedirectOutput.setConsole();
    }
}
