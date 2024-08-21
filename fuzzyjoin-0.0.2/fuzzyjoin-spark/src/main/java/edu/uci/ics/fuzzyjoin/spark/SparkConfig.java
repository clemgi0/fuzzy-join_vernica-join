package edu.uci.ics.fuzzyjoin.spark;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;
import scala.Tuple2;

public class SparkConfig {
    private File configFile;
    private String configDir = "src/main/resources/";
    private SparkConf conf;

    public SparkConf getSparkContext() throws IOException {
        // Initialize SparkConf with default properties
        conf = new SparkConf().setAppName("FuzzyJoinSpark")
                .set(Main.TOKENS_PACKAGE_PROPERTY, Main.TOKENS_PACKAGE_VALUE)
                .set(Main.TOKENS_LENGTHSTATS_PROPERTY, String.valueOf(Main.TOKENS_LENGTHSTATS_VALUE))
                .set(Main.RIDPAIRS_GROUP_CLASS_PROPERTY, Main.RIDPAIRS_GROUP_CLASS_VALUE)
                .set(Main.RIDPAIRS_GROUP_FACTOR_PROPERTY, String.valueOf(Main.RIDPAIRS_GROUP_FACTOR_VALUE))
                .set(Main.DATA_DIR_PROPERTY, "default-directory") // Default value
                .set(Main.DATA_RAW_PROPERTY, Main.DATA_RAW_VALUE)
                .set(Main.DATA_SUFFIX_INPUT_PROPERTY, "") // jsp si j'ai bien de les rajouter
                .set(Main.DATA_LENGTHSTATS_PROPERTY, ""); // ces deux là

        return conf;
    }

    public void readConfig(String configSubDir, String configFileName) throws IOException {
        // Read configuration file and override properties
        configFile = new File(configDir + configSubDir + configFileName);

        if (!configFile.exists()) {
            throw new RuntimeException("Configuration file not found: " + configFileName);
        }

        LogUtil.logStage("Reading configuration file: " +
                configFile.getAbsolutePath() + " | " + configFile.exists());

        try {
            // Parse the XML configuration file
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(configFile);
            doc.getDocumentElement().normalize();
            NodeList nodeList = doc.getElementsByTagName("property");

            // Override SparkConf properties with those from the XML file
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String name = element.getElementsByTagName("name").item(0).getTextContent();
                    String value = element.getElementsByTagName("value").item(0).getTextContent();
                    conf.set(name, value);
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to parse configuration file: " + configFileName, e);
        }
    }

    public void printAllProperties() {
        // Log and print the properties
        LogUtil.logStage("Print all properties");

        String ret = "All " + conf.logName() + "\n"
                + " Properties: {";

        for (Tuple2<String, String> prop : conf.getAll()) {
            ret += prop._1 + "=" + prop._2 + "\n";
        }

        ret += "}";
        System.out.println(ret);
    }

    public JavaRDD<String> readData(JavaSparkContext sc, String subDir) {
        // Read the records from HDFS
        String dataDir = sc.getConf().get(Main.DATA_DIR_PROPERTY);

        if (dataDir == null) {
            throw new UnsupportedOperationException(
                    "ERROR: fuzzyjoin.data.dir not set");
        }

        int dataCopy = Integer.parseInt(sc.getConf().get(Main.DATA_COPY_PROPERTY, "1"));
        String dataCopyFormatted = String.format("-%03d", dataCopy - 1);
        JavaRDD<String> data = sc.textFile(dataDir + "/" + subDir + dataCopyFormatted + "/part-00000");

        return data;
    }
}
