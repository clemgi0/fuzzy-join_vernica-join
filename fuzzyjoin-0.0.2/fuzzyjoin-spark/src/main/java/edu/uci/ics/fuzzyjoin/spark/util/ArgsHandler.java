package edu.uci.ics.fuzzyjoin.spark.util;

import java.io.File;
import java.util.Map;
import java.util.HashMap;

import edu.uci.ics.fuzzyjoin.spark.Main;

public class ArgsHandler {
    private static final Map<String, String> options = new HashMap<>();

    public static void printUsage() {
        System.err.println("\nUsage: spark-submit \\\n" +
                "--class PATH_TO_MAIN \\\n" +
                "--master YARN \\\n" +
                "PATH_TO_JAR \\\n" +
                "CONFIG_FILE STAGE \\\n" +
                "OPTIONS: \n" +
                "CONFIG_FILE: Path to the configuration XML file \n" +
                "STAGE: Stage to run (e.g., tokensbasic, ridpairsppjoin, recordpairsbasic, fuzzyjoin) \n" +
                "-Dfuzzyjoin.output.file=true/false: Optionally specify 'false' to log output to console, default is file output.txt.\n"
                +
                "-Dfuzzyjoin.savetime=true/false: Optionally specify 'true' to save times to the file times.txt.\n");
        System.exit(1);
    }

    public static void handleArg(String[] args) {
        if (args.length < 2) {
            printUsage();
        }

        // Parse arguments
        String configFile = args[0];
        if (!isValidConfigFile(configFile)) {
            System.err.println("\nInvalid configuration file specified: " + configFile);
            printUsage();
        }

        String stageName = args[1].toLowerCase();
        if (!isValidStage(stageName)) {
            System.err.println("\nInvalid stage specified: " + stageName);
            ArgsHandler.printUsage();
        }
    }

    public static String getConfigFile(String[] args) {
        return args[0];
    }

    public static String getStage(String[] args) {
        return args[1].toLowerCase();
    }

    private static boolean isValidConfigFile(String configFile) {
        File file = new File("src/main/resources/fuzzyjoin/" + configFile);
        return file.exists() && file.isFile() && file.canRead();
    }

    private static boolean isValidStage(String stageName) {
        return Main.STAGES.get(stageName.toLowerCase()) != null;
    }
}
