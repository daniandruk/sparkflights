package org.wipro.sparkflights;
import org.junit.Test;

import java.net.URISyntaxException;

public class WordCountTaskTest {
  @Test
  public void test() throws URISyntaxException {
    String inputFile = getClass().getResource("/airlines.dat").toURI().toString();
    new WordCountTask().run(inputFile);
  }
}
