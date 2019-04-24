package edu.uci.ics.cs221.index;

import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.analysis.ComposableAnalyzer;
import edu.uci.ics.cs221.analysis.PorterStemmer;
import edu.uci.ics.cs221.analysis.PunctuationTokenizer;
import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.storage.Document;
import org.junit.Test;


public class Team11MergeSearchTest {

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
    InvertedIndexManager index = InvertedIndexManager.createOrOpen("index_path", analyzer);

    @Test
    public void mergeSearchTest1() {
        index.DEFAULT_FLUSH_THRESHOLD = 5;

        for (Document doc : documents) {
            index.addDocument(doc);
            assert index.getNumSegments() < index.DEFAULT_FLUSH_THRESHOLD;
        }

    }
}
