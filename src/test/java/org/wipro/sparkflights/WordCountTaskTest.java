package org.wipro.sparkflights;

import org.junit.Test;

import java.net.URISyntaxException;

public class WordCountTaskTest {

    @Test
    public void test() throws URISyntaxException {
        String inputFile = getClass().getResource("/airlines.dat").toURI().toString();
        WordCountTask wordCountTask = new WordCountTask();
        wordCountTask.run(inputFile);

    }
}
