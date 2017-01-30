package org;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

public class SparkWordCount implements Serializable{

    private static final Pattern SPACE = Pattern.compile(" ");
    SparkSession spark;

    public SparkWordCount (String environment) {
        initialize(environment);
    }

    /**
     * Removing email headers (or most of them here)
     * this could proably be further optimized in the future but was all I had time for now.
     * @param lines
     * @return
     */
    JavaRDD<String> removeEmailHeaders(JavaRDD<String> lines) {
        return lines.filter(a -> !a.contains("Message-ID:") && !a.contains("From:") &&
                !a.contains("To:") && !a.contains("Mime-Version:") && !a.contains("Content-Type:")
                && !a.contains("Content-Transfer-Encoding:") && !a.contains("X-Folder:") && !a.contains("X-cc:")
                && !a.contains(("X-From:")) && !a.contains("X-To:") && !a.contains("X-Origin:")
                && !a.contains("X-FileName:"));
    }


    /**
     * break line of text into individual words, remove punctuation and convert to lower case
     * @param lines
     * @return
     */
    public JavaRDD<String> getWordsFromStringLine (JavaRDD<String> lines) {
        return lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                //remove punctuation and convert to lowercase
                String r = s.replaceAll("\\p{P}", "").toLowerCase();
                return Arrays.asList(r.split(" ")).iterator(); }
        });
    }

    public JavaRDD<String> loadEnronEmailDirectory(String inputPath) {
        //this pattern is in place just to load the enron email files and to make input easier
        return spark.read().textFile(inputPath + "/*/*/*.").javaRDD();
    }

    /**
     * use this to load a regular text from a normal path
     * @param inputPath
     * @return
     */
    public JavaRDD<String> loadStandardDirectory(String inputPath) {
        return spark.read().textFile(inputPath).javaRDD();
    }

    /**
     * Load stop words from text file, and remove them from the rdd of words
     * @param words
     * @param stopWordsPath
     * @return
     */
    public JavaRDD<String> removeStopWords(JavaRDD<String> words, String stopWordsPath) {
        //load stop words
        JavaRDD<String> stopLines = spark.read().textFile(stopWordsPath).javaRDD();
        JavaRDD<String> stopWords = stopLines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());

        //remove words that are less than 2 characters in length
        JavaRDD<String> longerWords = words.filter(a -> a.length() > 1);
        //Remove all stop words from all the words in the
        return longerWords.subtract(stopWords);
    }

    /**
     * count words from JavaRDD String
     * @param words
     * @return
     */
    JavaPairRDD<String, Integer> performWordCount(JavaRDD<String> words ) {
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        return counts;
    }

    void initialize (String environment) {

        if (environment.equalsIgnoreCase("production")) {
            //if were to be run in different environments
            spark = SparkSession
                    .builder()
                    .master("Production vars")
                    .appName("JavaWordCount")
                    .getOrCreate();
        } else {
            //defaults to a local run
            spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("JavaWordCount")
                    .getOrCreate();
        }
    }

    /**
     * sort words in order by most frequently occurring, and also remove any words that occur less than 10 times.
     * @param counts
     * @return
     */
    public JavaPairRDD<Tuple2<Integer, String>, Integer> sortAndFilter(JavaPairRDD<String, Integer> counts) {

        //filter out minimum 10 word occurances
        counts = counts.filter(new Function<Tuple2<String,Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                return v1._2() > 10;
            }
        });

        //now sort the result
        // setting value to null, since it won't be used anymore
        JavaPairRDD<Tuple2<Integer, String>, Integer> countInKey =
                counts.mapToPair(a -> new Tuple2(new Tuple2<Integer, String>(a._2(), a._1()), ""));

        // sort by num of occurences
        JavaPairRDD<Tuple2<Integer, String>, Integer> wordSortedByCount = countInKey.sortByKey(new CompareWords(), false);
        return wordSortedByCount;
    }

    /**
     * save to file.  the number of results can be changed with the input parameter as desired
     * @param wordsSortedByCount
     * @param numberFilesResultSize
     * @param outputPath
     */
    public void saveToFile(JavaPairRDD<Tuple2<Integer, String>, Integer> wordsSortedByCount, int numberFilesResultSize, String outputPath) {
        wordsSortedByCount.coalesce(numberFilesResultSize).saveAsTextFile(outputPath);
    }

    public void closeSession() {
        spark.close();
    }
}