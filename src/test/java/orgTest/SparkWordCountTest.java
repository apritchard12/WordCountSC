package orgTest;

import junit.framework.Assert;
import org.SparkWordCount;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class SparkWordCountTest {

    String projectDir = System.getProperty("user.dir");
    String resources = "/src/main/resources/";

    @Test
    public void wordCount() {

        SparkWordCount sparkWordCount = new SparkWordCount("local");

        JavaRDD<String> lines = sparkWordCount.loadStandardDirectory(projectDir + resources + "inputWords");

        //split the string into word
        JavaRDD<String> words = sparkWordCount.getWordsFromStringLine(lines);
        //check split is done correctly
        assertEquals("Word count parses correctly: ", 13, words.count());

        //check that words are filtered correctly by removing stop words

        JavaRDD<String> filtered = sparkWordCount.removeStopWords(words, projectDir + resources + "stopWords");

        //test filtered size
        //should have removed 2 words, 'a', and 'subject'
        assertEquals("Stop words removed test: ", 11, filtered.count());

        List<String> list = filtered.collect();
        for (String a: list)
            System.out.println(a);

        sparkWordCount.closeSession();
    }
}
