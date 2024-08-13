package edu.uci.ics.fuzzyjoin.spark;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.spark.logging.LogUtil;

public class SparkConfig {
    private File configFile;
    private String configFileName = "src/main/resources/fuzzyjoin/";
    private SparkConf conf;

    public SparkConfig(String name) {
        configFileName += name;
    }

    public SparkConf getSparkContext() throws IOException {
        // Initialize SparkConf with default properties
        conf = new SparkConf().setAppName("FuzzyJoinSpark")
                .set(Main.TOKENS_PACKAGE_PROPERTY, Main.TOKENS_PACKAGE_VALUE)
                .set(Main.TOKENS_LENGTHSTATS_PROPERTY, String.valueOf(Main.TOKENS_LENGTHSTATS_VALUE))
                .set(Main.RIDPAIRS_GROUP_CLASS_PROPERTY, Main.RIDPAIRS_GROUP_CLASS_VALUE)
                .set(Main.RIDPAIRS_GROUP_FACTOR_PROPERTY, String.valueOf(Main.RIDPAIRS_GROUP_FACTOR_VALUE))
                .set(Main.DATA_DIR_PROPERTY, "default-directory") // Default value
                .set(Main.DATA_RAW_PROPERTY, Main.DATA_RAW_VALUE);

        // Read configuration file and override properties
        configFile = new File(configFileName);
        if (!configFile.exists()) {
            throw new RuntimeException("Configuration file not found: " + configFileName);
        }

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

        return conf;
    }

    public void printProperties(JavaSparkContext sc) {
        // Log and print the properties
        LogUtil.logStage("Print properties");
        String ret = "Main " + sc.appName() + "\n"
                + " Input Path: {" + conf.get(Main.DATA_DIR_PROPERTY) + "}\n"
                + " Properties: {";
        String[][] properties = new String[][] {
                new String[] { FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY, FuzzyJoinConfig.SIMILARITY_NAME_VALUE },
                new String[] { FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                        String.valueOf(FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE) },
                new String[] { FuzzyJoinConfig.TOKENIZER_PROPERTY, FuzzyJoinConfig.TOKENIZER_VALUE },
                new String[] { Main.TOKENS_PACKAGE_PROPERTY, Main.TOKENS_PACKAGE_VALUE },
                new String[] { Main.TOKENS_LENGTHSTATS_PROPERTY, String.valueOf(Main.TOKENS_LENGTHSTATS_VALUE) },
                new String[] { Main.RIDPAIRS_GROUP_CLASS_PROPERTY, Main.RIDPAIRS_GROUP_CLASS_VALUE },
                new String[] { Main.RIDPAIRS_GROUP_FACTOR_PROPERTY, String.valueOf(Main.RIDPAIRS_GROUP_FACTOR_VALUE) },
                new String[] { FuzzyJoinConfig.DATA_TOKENS_PROPERTY, "" },
                new String[] { Main.DATA_JOININDEX_PROPERTY, "" },
        };
        for (int crt = 0; crt < properties.length; crt++) {
            if (crt > 0) {
                ret += "\n ";
            }
            ret += properties[crt][0] + "=" + conf.get(properties[crt][0], properties[crt][1]);
        }
        ret += "}";
        System.out.println(ret);
    }
}
