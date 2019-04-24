package edu.uci.ics.cs221.index;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.storage.Document;
import org.junit.After;
import org.junit.Test;

import java.io.File;

public class Team11MergeSearchTest {
    String indexPath = "index_path";

    // Back to December by Taylor Swift
    Document[] documents = new Document[] {
        new Document("import edu uci ics cs221 analysis  Analyzer"),
        new Document("import edu uci ics cs221 analysis  ComposableAnalyzer"),
        new Document("import edu uci ics cs221 analysis  PorterStemmer"),
        new Document("import edu uci ics cs221 analysis  PunctuationTokenizer"),
        new Document("import edu uci ics cs221 index     inverted            InvertedIndexManager"),
        new Document("import edu uci ics cs221 storage   Document")
    };

    Analyzer analyzer = new ComposableAnalyzer(new PunctuationTokenizer(), new PorterStemmer());
    InvertedIndexManager index = InvertedIndexManager.createOrOpen(indexPath, analyzer);

    @After
    public void clean() {
        try {
            File file = new File(indexPath);

            if (file.delete()) {
                System.out.println("File deleted successfully");
            } else {
                System.out.println("Failed to delete the file");
            }
        } catch (Exception e) {
            System.out.println("Something went wrong when deleting file");
        }
    }

    @Test
    public void mergeSearchTest1() {
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1;
        InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 4;

        for (Document doc : documents) {
            index.addDocument(doc);
            assert index.getNumSegments() <= InvertedIndexManager.DEFAULT_MERGE_THRESHOLD;
        }
    }

    @Test
    public void mergeSearchTest2() {
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1;
        InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 2;

        manager.addDocument(documents[0]);
        manager.addDocument(documents[1]);
        int expectedNumSegments = 1;
        assertEquals(expectedNumSegments, manager.getNumSegments());
    }
    @After
    public void delete1() {
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 1000;
        InvertedIndexManager.DEFAULT_MERGE_THRESHOLD = 8;
    }
}
