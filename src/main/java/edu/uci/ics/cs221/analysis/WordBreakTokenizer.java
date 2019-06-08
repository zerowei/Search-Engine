package edu.uci.ics.cs221.analysis;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 *
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 *
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 *
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 *
 * Requirements:
 *  - Use Dynamic Programming for efficiency purposes.
 *  - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 *      The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 *  - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 *  - Stop words should be removed.
 *  - If there's no possible way to break the string, throw an exception.
 *
 */
public class WordBreakTokenizer implements Tokenizer {

    public Map<String, Long> wordsFreq = new HashMap();
    public static Set<String> punctuations = new HashSet<>();
    private double totalFrequency = 0;
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));
            for (String dict : dictLines){
                List<String> WordsFreq = Arrays.asList(dict.toLowerCase().split(" "));
                // Hack to deal with the invisible char at the beginning of the file
                // Related Slack message is at https://uci-cs221-s19.slack.com/archives/CHM5W2K6G/p1554440079005900
                if (WordsFreq.get(0).startsWith("\uFEFF")){
                    WordsFreq.set(0, WordsFreq.get(0).substring(1));
                }
                Long fre =  Long.parseLong(WordsFreq.get(1));
                wordsFreq.put(WordsFreq.get(0),fre);
                totalFrequency += fre;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //System.out.println();
    }


    public List<String> tokenize(String text) {
        int len = text.length();
        if (len == 0) {
            return new ArrayList<>();
        }

        for (String str: punctuations) {
            if (text.contains(str))
                throw new RuntimeException("Punctuations should not be input to wordBreakTokenizer");
        }

        text = text.toLowerCase();
        String[][] cache = new String[len][len];
        double[][] probabilityMatrix = new double[len][len];

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < len; j++) {
                cache[i][j] = "";
                probabilityMatrix[i][j] = -10000;
            }
        }

        for (int l = 1; l <= len; l++){
            for (int i = 0; i <= len-l; i++) {
                int j = i+l-1; // try to break s[i, j] and s[j] is included

                String subStr = text.substring(i, j+1);
                if (wordsFreq.containsKey(subStr) && probabilityMatrix[i][j] < Math.log(wordsFreq.get(subStr)/totalFrequency)) {
                    probabilityMatrix[i][j] = Math.log(wordsFreq.get(subStr)/totalFrequency);
                    cache[i][j] = subStr;
                }
                // Even the substring is in the dictionary, we still need to try to break it
                // -> test case for the situation that str is in dictionary but it has a higher probability to split it?

                for (int k = i; k <= j-1; k++) {
                    if (cache[i][k].length() > 0 && cache[k+1][j].length() > 0
                            && probabilityMatrix[i][j] < probabilityMatrix[i][k] + probabilityMatrix[k+1][j]) {
                        probabilityMatrix[i][j] = probabilityMatrix[i][k] + probabilityMatrix[k+1][j];
                        cache[i][j] = cache[i][k] + " " + cache[k+1][j];
                    }
                }
            }
        }

        if (cache[0][len-1].length() == 0 && probabilityMatrix[0][len-1] < 0) {
            throw new RuntimeException("String cannot be broken into words according to the dictionary");
        }
        // Use ArrayList to enable modification
        // Reference: https://stackoverflow.com/questions/6026813/converting-string-array-to-java-util-list
        List<String> results = new ArrayList<>(Arrays.asList(cache[0][len-1].split(" ")));
        results.removeAll(StopWords.stopWords);
        //System.out.println(results);
        return results;
    }
}
