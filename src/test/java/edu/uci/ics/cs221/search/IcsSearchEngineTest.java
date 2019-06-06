package edu.uci.ics.cs221.search;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This test is for Project 4 Task 2 - PageRank and Search ICS webpages.
 * Please DO NOT commit the ICS webpage files into your Git repository. When running the tests,
 * the TA will copy the files to the "webpages" folder in root directory of your project.
 *
 */
public class IcsSearchEngineTest {

    @ClassRule
    public static Timeout classTimeout = Timeout.seconds(900);

    static Path webPagesPath = Paths.get("./webpages");
    static Path indexPath = Paths.get("./index/IcsSearchEngineTest");
    static Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());

    static InvertedIndexManager invertedIndexManager;
    static IcsSearchEngine icsSearchEngine;

    static BiMap<Integer, String> idUrlMap;

    @BeforeClass
    public static void setup() throws Exception {
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 2000;

        invertedIndexManager = InvertedIndexManager.createOrOpen(indexPath.toString(), analyzer);
        icsSearchEngine = IcsSearchEngine.createSearchEngine(webPagesPath, invertedIndexManager);

        icsSearchEngine.writeIndex();
        icsSearchEngine.computePageRank(100);

        idUrlMap = HashBiMap.create();
        Files.readAllLines(webPagesPath.resolve("url.tsv")).stream().map(line -> line.split("\\s")).forEach(line -> {
            idUrlMap.put(Integer.parseInt(line[0].trim()), line[1].trim());
        });
    }

    @AfterClass
    public static void cleanup() throws Exception {
        deleteDirectory(indexPath.toFile());
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1000;
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }


    /**
     * Trying to search the ISG homepage.
     */
    @Test
    public void testCombinePageRankTfIdf() {

        Iterator<Pair<Document, Double>> resultIterator = icsSearchEngine.searchQuery(
                Arrays.asList("ISG", "Bren", "School", "UCI"),
                100, 100.0);
        ImmutableList<Pair<Document, Double>> resultList = ImmutableList.copyOf(resultIterator);

        // first result should be "isg.ics.uci.edu"
        Assert.assertEquals("isg.ics.uci.edu", getDocumentUrl(resultList.get(0).getLeft().getText()));

        // top 10 should have URLs starting "hobbes.ics.uci.edu"
        Assert.assertTrue(resultList.stream().limit(10).map(p -> getDocumentUrl(p.getLeft().getText()))
                .anyMatch(p -> p.contains("hobbes.ics.uci.edu")));

        // top 50 should have URL "ipubmed2.ics.uci.edu"
        Assert.assertTrue(resultList.stream().limit(50).map(p -> getDocumentUrl(p.getLeft().getText()))
                .anyMatch(p -> p.equals("ipubmed2.ics.uci.edu")));

    }


    /**
     * Sets a very high page rank weight and search keyword "anteater".
     * The "wics.ics.uci.edu" webpage has very high ranking and should appear in the top.
     */
    @Test
    public void testCombineRankHighPageRankWeight() {
        Iterator<Pair<Document, Double>> resultIterator = icsSearchEngine.searchQuery(Arrays.asList("anteater"),
                10, 1000000000.0);
        ImmutableList<Pair<Document, Double>> resultList = ImmutableList.copyOf(resultIterator);
        Assert.assertEquals(10, resultList.size());
        Assert.assertTrue(resultList.stream().limit(3).map(p -> p.getLeft())
                .anyMatch(doc -> doc.getText().contains("wics.ics.uci.edu")));
    }

    /**
     * Sets a very page rank weight to 0. Result should be the same as TF-IDF
     */
    @Test
    public void testCombineRankZeroPageRankWeight() {
        Iterator<Pair<Document, Double>> resultIterator = icsSearchEngine.searchQuery(Arrays.asList("anteater"),
                100, 0.0);
        ImmutableList<Pair<Document, Double>> resultListCombined = ImmutableList.copyOf(resultIterator);

        Iterator<Pair<Document, Double>> resultIteratorTfIdf = invertedIndexManager.searchTfIdf(Arrays.asList("anteater"), 100);
        ImmutableList<Pair<Document, Double>> resultListTfIdf = ImmutableList.copyOf(resultIteratorTfIdf);

        Assert.assertEquals(resultListTfIdf, resultListCombined);
    }


    public static String getDocumentUrl(String text) {
        String[] result = text.split("\n");
        return result[1].trim();
    }


}
