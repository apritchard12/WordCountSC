package org;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class Main {

    public static void main (String[] args ) {

        if (args.length < 3) {
            System.out.println("Required arguments missing:");
            System.out.println("intputPath: '/input/path/maildir', /stop/words/path/stopWords.txt, outputPath");
            System.exit(1);
        }

        //initialize
        SparkWordCount sparkWordCount = new SparkWordCount("local");

        //load email directory  ** IMPORTANT - top level directory only, pattern will match subfolders in this case
        JavaRDD<String> lines = sparkWordCount.loadEnronEmailDirectory(args[0]);

        //filter email headers
        JavaRDD<String> nonHeaderLines = sparkWordCount.removeEmailHeaders(lines);
        //convert lines of text into rdd of words
        JavaRDD<String> words = sparkWordCount.getWordsFromStringLine(nonHeaderLines);

        //Remove Stop Words
        JavaRDD<String> filteredWords = sparkWordCount.removeStopWords(words, args[1]);

        //process word count
        JavaPairRDD<String, Integer> counts = sparkWordCount.performWordCount(filteredWords);

        // sort by num of occurences
        JavaPairRDD<Tuple2<Integer, String>, Integer> wordsSortedByCount = sparkWordCount.sortAndFilter(counts);

        //save data to file, include int number of files you'd like saved
        sparkWordCount.saveToFile(wordsSortedByCount, 1, args[2]);

        //close session
        sparkWordCount.closeSession();
    }
}