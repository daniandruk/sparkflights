package org.wipro.sparkflights;

import org.junit.Test;

import java.net.URISyntaxException;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTaskTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTaskTest.class);

    @Test
    public void test() throws URISyntaxException {
        String inputFile = getClass().getResource("/airlines.dat").toURI().toString();
        WordCountTask wordCountTask = new WordCountTask();
        JavaPairRDD<String, String> javaPairRDD = wordCountTask.run(inputFile);

        javaPairRDD.foreach(result -> {
            int count = result._2.split(",").length;
            LOGGER.info(String.format("Word [%s] count [%d] list [%s]", result._1(), count, result._2));
        }
        );
    }
}
