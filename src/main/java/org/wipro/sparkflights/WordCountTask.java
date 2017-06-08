package org.wipro.sparkflights;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTask {

    /**
     * We use a logger to print the output. Sl4j is a common library which works
     * with log4j, the logging system used by Apache Spark.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

    /**
     * This is the entry point when the task is called from command line with
     * spark-submit.sh. See {
     *
     * @see http://spark.apache.org/docs/latest/submitting-applications.html}
     */
    public static void main(String[] args) throws URISyntaxException, InterruptedException {
        String inputFile = WordCountTask.class.getResource("/airlines.dat").toURI().toString();
        if (args.length > 0) {
            inputFile = args[0];
        }

        WordCountTask wordCountTask = new WordCountTask();
        wordCountTask.run(inputFile);

//        List<Tuple2<String,String>> list=javaPairRDD.collect();
//        
//        list.forEach((tuple2) -> {
//            int count = tuple2._2.split(",").length;
//            LOGGER.info(String.format("Word [%s] count [%d] list [%s]", tuple2._1(), count, tuple2._2));
//        });
//        javaPairRDD.foreach(result -> {
//            int count = result._2.split(",").length;
//            LOGGER.info(String.format("Word [%s] count [%d] list [%s]", result._1(), count, result._2));
//        }
//        );
    }

    /**
     * The task body
     */
    public void run(String inputFilePath) {
        /*
     * This is the address of the Spark cluster. We will call the task from WordCountTest and we
     * use a local standalone cluster. [*] means use all the cores available.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
         */
        String master = "local[*]";

        /*
     * Initialises a Spark context.
         */
        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile(inputFilePath).flatMap(text -> Arrays.asList(text.replace("\"", "").split("\\n")).iterator())
                .mapToPair(word -> new Tuple2<>(word.split(",")[6], word.split(",")[1]))
                .reduceByKey((a, b) -> a + "," + b)
                .mapToPair((Tuple2<String, String> item) -> item.swap())
                .sortByKey((Comparator<String> & Serializable) (String o1, String o2) -> {
                    Integer size1 = o1.split(",").length;
                    Integer size2 = o2.split(",").length;
                    return Integer.compare(size1, size2);
                }, false)
                .mapToPair((Tuple2<String, String> item) -> item.swap())
                .collect()
                .forEach((tuple2) -> {
                    int count = tuple2._2.split(",").length;
                    LOGGER.info(String.format("Word [%s] count [%d] list [%s]", tuple2._1(), count, tuple2._2));
                });
    }

//        SparkConf conf = new SparkConf()
//                .setAppName(WordCountTask.class.getName())
//                .setMaster(master);
//        JavaSparkContext context = new JavaSparkContext(conf);
//
//        JavaRDD<String> javaRDD = context.textFile(inputFilePath).flatMap(text -> Arrays.asList(text.replace("\"", "").split("\\n")).iterator());
//        JavaPairRDD<String, String> javaPairRDD1 = javaRDD.mapToPair(word -> new Tuple2<>(word.split(",")[6], word.split(",")[1]));
//        JavaPairRDD<String, String> javaPairRDD2 = javaPairRDD1.reduceByKey((a, b) -> a + "," + b);
//
//        JavaPairRDD<String, String> swappedPair2 = javaPairRDD2.mapToPair((Tuple2<String, String> item) -> item.swap());
//
//        JavaPairRDD<String, String> javaPairRDD3 = swappedPair2.sortByKey((Comparator<String> & Serializable) (String o1, String o2) -> {
//            Integer size1 = o1.split(",").length;
//            Integer size2 = o2.split(",").length;
//            return Integer.compare(size1, size2);
//        });
//
//        JavaPairRDD<String, String> swappedPair3 = javaPairRDD3.mapToPair((Tuple2<String, String> item) -> item.swap());
//
//        swappedPair3.foreach(result -> {
//            int count = result._2.split(",").length;
//            LOGGER.info(String.format("Word [%s] count [%d] list [%s]", result._1(), count, result._2));
//        }
//        );     
//    }
}
