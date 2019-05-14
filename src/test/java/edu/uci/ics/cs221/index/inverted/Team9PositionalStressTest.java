package edu.uci.ics.cs221.index.inverted;

import com.google.common.collect.Table;
import com.sun.tools.javac.util.Name;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class Team9PositionalStressTest {
    private ComposableAnalyzer analyzer;
    static List<String> allDocuments;
    private String indexFolder  = "./index/Team9PositionalStressTest/";;
    private InvertedIndexManager invertedIndex;
    private Compressor compressor;
    private static final int TOTALNUM = 50000;
    private static final String textUrl = "https://github.com/DanniUCI/spring19-cs221-project/blob/master/team9StressTest.txt";

    @Test(timeout = 600000)
    public void setupAndRun(){
        allDocuments = getOnlineTextFile(textUrl);
        analyzer = new ComposableAnalyzer(new PunctuationTokenizer(),new PorterStemmer());
        compressor = new NaiveCompressor();
        invertedIndex = InvertedIndexManager.createOrOpenPositional(indexFolder, analyzer, compressor);
        PageFileChannel.resetCounters();
        for(int i=0;i<TOTALNUM;i++){
            invertedIndex.addDocument(new Document(allDocuments.get(i%allDocuments.size())));
        }
        assertTrue(PageFileChannel.writeCounter>=TOTALNUM/invertedIndex.DEFAULT_FLUSH_THRESHOLD);

        try {
            test1();
        } catch (Throwable e) {
            System.out.println("Team9StressTest test1 FAILED");
            e.printStackTrace();
        }

        try {
            test2();
        } catch (Throwable e) {
            System.out.println("Team9StressTest test2 FAILED");
            e.printStackTrace();
        }

        try {
            test3();
        } catch (Throwable e) {
            System.out.println("Team9StressTest test3 FAILED");
            e.printStackTrace();
        }

    }

    // document contains 1530 times "h"
    // In this test, we will test the correctness of the size of document size and position list size
    public void test1(){
        PageFileChannel.resetCounters();
        PositionalIndexSegmentForTest segment = invertedIndex.getIndexSegmentPositional(0);
        Map<Integer, Document> docs = segment.getDocuments();
        Table<String, Integer, List<Integer>> positions = segment.getPositions();
        List<Integer> posList = positions.get("h", 0);

        Assert.assertEquals(invertedIndex.getNumSegments(), InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD/2);
        Assert.assertEquals(posList.size(), 1530);

        assertTrue(PageFileChannel.readCounter<=3);
    }


    // test searchPhraseQuery() function. Search a long list of phrase
    // result should contain TOTALNUM document

    public void test2(){
        PageFileChannel.resetCounters();
        ArrayList<String> keyPhrase = new ArrayList<>(Arrays.asList("cat", "felis", "catus", "small", "carnivorous", "mammal", "domesticated", "species", "family", "felidae", "often", "referred", "domestic", "cat", "distinguish", "wild", "members", "family", "cat", "either", "house", "cat", "kept", "pet", "feral", "cat", "freely", "ranging", "avoiding", "human", "contact", "house", "cat", "valued", "humans", "companionship", "ability", "hunt", "rodents", "60", "cat", "breeds", "recognized", "various", "cat", "registries"));
        Iterator<Document> result = invertedIndex.searchPhraseQuery(keyPhrase);
        int count = 0;
        while(result.hasNext()){
            result.next();
            count++;
        }
        assertTrue(PageFileChannel.readCounter > TOTALNUM);
        assertEquals(count, TOTALNUM);

    }


    //Test searchPhraseQuery with phrase "cat dog", result should be empty.
    public void test3(){
        Iterator<Document> result1 = invertedIndex.searchPhraseQuery(Arrays.asList("cat","dog"));
        assertTrue(!result1.hasNext());
    }


    @After
    public void tearDown() {
        // local storage folder is a flat folder which doesn't contain sub folder
        // In case of any exception, that will be thrown out.
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1000;
        File localStorageFolder = new File(indexFolder);
        for (File file : localStorageFolder.listFiles()) {
            file.delete();
        }
        localStorageFolder.delete();
    }

    //Get our large resources text file from a URL
    private static List<String> getOnlineTextFile(String URL){
        List<String> result = new ArrayList<>();
        try {
            java.net.URL url = new URL(URL);
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String s;
            while ((s = reader.readLine()) != null) {
                result.add(s);
            }
            reader.close();
        }
        catch (Exception e){
            throw new RuntimeException(e);
        }
        return result;
    }
}
