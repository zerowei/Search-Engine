package edu.uci.ics.cs221.search;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Math.min;

public class IcsSearchEngine {
    private Path documentDirectory;
    private InvertedIndexManager indexManager;

    private int numWebpages;
    private List<List<Integer>> incomingEdges;
    private List<Integer> outDegrees;
    private List<Double> currentPR;


    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     *
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        this.documentDirectory = documentDirectory;
        this.indexManager = indexManager;
    }

    private class SortbyFileName implements Comparator<File>
    {
        public int compare(File a, File b)
        {
            return Integer.parseInt(a.getName()) - Integer.parseInt(b.getName());
        }
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        File cleanedFile = documentDirectory.resolve("cleaned").toFile();
        File[] files = cleanedFile.listFiles();
        Arrays.sort(files, new SortbyFileName());

        numWebpages = files.length;
        //System.out.println("num web pages " + numWebpages);

        for (int i = 0; i < numWebpages; i++) {
            File file = files[i];
            try {
                //System.out.println(file.getPath());
                String content = new String(Files.readAllBytes(Paths.get(file.getPath())));
                indexManager.addDocument(new Document(content));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Computes the page rank score from the "id-graph.tsv" file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank(int numIterations) {
        File cleanedFile = documentDirectory.resolve("cleaned").toFile();
        File[] files = cleanedFile.listFiles();
        Arrays.sort(files, new SortbyFileName());

        numWebpages = files.length;
        //System.out.println("num web pages " + numWebpages);

        incomingEdges = new ArrayList<>();
        outDegrees = new ArrayList<>();
        currentPR = new ArrayList<>();
        for (int i = 0; i < numWebpages; i++) {
            incomingEdges.add(new ArrayList<>());
            outDegrees.add(0);
            currentPR.add(1.0);
        }

        try {
            Files.readAllLines(documentDirectory.resolve("id-graph.tsv")).stream().map(line -> line.split("\\s")).forEach(line -> {
                int from = Integer.parseInt(line[0].trim());
                int to = Integer.parseInt(line[1].trim());

                incomingEdges.get(to).add(from);
                outDegrees.set(from, outDegrees.get(from) + 1);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        double maxPR = -1, minPR = numWebpages * 10;
        for (int iterNum = 0; iterNum < numIterations; iterNum++) {
            //System.out.println("iter " + iterNum);
            maxPR = -1;
            minPR = numWebpages * 10;

            List<Double> newPR = new ArrayList<>();
            for (int i = 0; i < numWebpages; i++) {
                newPR.add(0.0);
            }

            for (int i = 0; i < numWebpages; i++) {
                double sumIncomingPR = 0.0;
                for (int incomeId: incomingEdges.get(i)) {
                    sumIncomingPR += currentPR.get(incomeId) / outDegrees.get(incomeId).doubleValue();
                }

                double DAMPING_FACTOR = 0.85;
                double value = (1-DAMPING_FACTOR) + DAMPING_FACTOR * sumIncomingPR;
                newPR.set(i, value);

                if (value < minPR) {
                    minPR = value;
                }
                if (value > maxPR) {
                    maxPR = value;
                }
            }

            /*
            System.out.print("iter " + iterNum + " ");
            for (int i = 0; i < numWebpages; i++) {
                System.out.print(i + " : " + newPR.get(i) + "\t");
            }
            System.out.println();
             */

            currentPR = newPR;
        }

        for (int i = 0; i < numWebpages; i++) {
            currentPR.set(i, (currentPR.get(i) - minPR) / maxPR);
        }
    }

    /**
     * Gets the page rank score of all documents previously computed. Must be called after `cmoputePageRank`.
     * Returns an list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        List<Pair<Integer, Double>> result = new ArrayList<>();

        for (int i = 0; i < numWebpages; i++) {
            result.add(new Pair<>(i, currentPR.get(i)));
        }

        result.sort((m1, m2) -> (m1.getRight() > m2.getRight()) ? -1 : (m1.getRight() < m2.getRight()) ? 1 : 0);

        return result;
    }

    public static int getDocumentId(String text) {
        String[] result = text.split("\n");
        return Integer.parseInt(result[0].trim());
    }

    /**
     * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and PageRank.
     *
     * The search process should first retrieve ALL the top documents from the InvertedIndex by TF-IDF rank,
     * by calling `searchTfIdf(query, null)`.
     *
     * Then the corresponding PageRank score of each document should be retrieved. (`computePageRank` will be called beforehand)
     * For each document, the combined score is  tfIdfScore + pageRankWeight * pageRankScore.
     *
     * Finally, the top K documents of the combined score are returned. Each element is a pair of <Document, combinedScore>
     *
     *
     * Note: We could get the Document ID by reading the first line of the document.
     * This is a workaround because our project doesn't support multiple fields. We cannot keep the documentID in a separate column.
     */
    public Iterator<Pair<Document, Double>> searchQuery(List<String> query, int topK, double pageRankWeight) {
        Iterator<Pair<Document, Double>> it = indexManager.searchTfIdf(query, null, false);

        List<Pair<Document, Double>> result = new ArrayList<>();
        while (it.hasNext()) {
            Pair<Document, Double> pair = it.next();
            int documentId = getDocumentId(pair.getLeft().getText());
            //System.out.println("document id " + getDocumentId(pair.getLeft().getText()));
            //System.out.println("right " + pair.getRight() + " PR " + currentPR.get(documentId));
            double score = pair.getRight() + pageRankWeight * currentPR.get(documentId);
            result.add(new Pair<>(pair.getLeft(), score));
        }

        result.sort(new InvertedIndexManager.CompareResults());
        return result.subList(0, min(topK, result.size())).iterator();
    }
}
