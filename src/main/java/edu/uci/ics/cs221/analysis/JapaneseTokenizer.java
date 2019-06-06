package edu.uci.ics.cs221.analysis;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JapaneseTokenizer {

    public Map<String, Integer> Words = new HashMap<>();
    public List<String> result = new ArrayList<>();
    public static Set<String> punctuations = new HashSet<>();
    private int totalFre = 0;

    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public JapaneseTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("JapaneseDic.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()), Charset.forName("GBK"));
            for (String dict : dictLines) {
                List<String> japanWords = Arrays.asList(dict.split("\\s+"));
                Integer fre = Integer.parseInt(japanWords.get(0));
                totalFre += fre;
                Words.put(japanWords.get(1), fre);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        int len = text.length();
        if (len == 0) {
            return new ArrayList<>();
        }
        for (String str : punctuations) {
            if (text.contains(str))
                throw new RuntimeException("Punctuations should not be input to JapaneseTokenizer");
        }

        String[][] cache = new String[len][len];
        double[][] probabilityMatrix = new double[len][len];

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < len; j++) {
                cache[i][j] = "";
                probabilityMatrix[i][j] = -1000;
            }
        }

        for (int l = 1; l <= len; l++) {
            for (int i = 0; i <= len - l; i++) {
                int j = i + l - 1; // try to break s[i, j] and s[j] is included
                String subStr = text.substring(i, j + 1);
                if (Words.containsKey(subStr) && probabilityMatrix[i][j] < Math.log(Words.get(subStr) *1.0 / totalFre)) {
                    probabilityMatrix[i][j] = Math.log(Words.get(subStr) *1.0 / totalFre);
                    cache[i][j] = subStr;
                }
                // Even the substring is in the dictionary, we still need to try to break it
                // -> test case for the situation that str is in dictionary but it has a higher probability to split it?

                for (int k = i; k <= j - 1; k++) {
                    if (cache[i][k].length() > 0 && cache[k + 1][j].length() > 0
                            && probabilityMatrix[i][j] < probabilityMatrix[i][k] + probabilityMatrix[k + 1][j]) {
                        probabilityMatrix[i][j] = probabilityMatrix[i][k] + probabilityMatrix[k + 1][j];
                        cache[i][j] = cache[i][k] + " " + cache[k + 1][j];
                    }
                }
            }
        }

        if (cache[0][len - 1].length() == 0 && probabilityMatrix[0][len - 1] < 0) {
            throw new RuntimeException("String cannot be broken into words according to the dictionary");
        }

        List<String> results = new ArrayList<>(Arrays.asList(cache[0][len - 1].split(" ")));
        results.removeAll(JapaneseStopWords.JapanesestopWords);
        System.out.println(results);
        return results;
    }
}
