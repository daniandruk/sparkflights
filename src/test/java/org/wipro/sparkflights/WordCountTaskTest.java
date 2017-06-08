package org.wipro.sparkflights;

import org.junit.Test;

import java.net.URISyntaxException;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTaskTest {

    @Test
    public void test() throws URISyntaxException {
        String inputFile = getClass().getResource("/airlines.dat").toURI().toString();
        WordCountTask wordCountTask = new WordCountTask();
        wordCountTask.run(inputFile);

    }
}
