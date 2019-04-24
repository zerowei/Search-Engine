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
        new Document("This morning I ate eggs"),
        new Document("I'm so glad you made time to see me"),
        new Document("How's life, tell me how's your family?"),
        new Document("I haven't seen them in a while"),
        new Document("You've been good, busier then ever"),
        new Document("We small talk, work and the weather"),
        new Document("Your guard is up and I know why")
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
        InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD = 5;

        for (Document doc : documents) {
            index.addDocument(doc);
            index.mergeAllSegments();
            assert index.getNumSegments() < InvertedIndexManager.DEFAULT_FLUSH_THRESHOLD;
        }
    }
}
