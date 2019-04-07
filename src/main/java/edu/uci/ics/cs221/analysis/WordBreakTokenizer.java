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

    public HashMap hm = new HashMap();

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
                hm.put(WordsFreq.get(0), Long.parseLong(WordsFreq.get(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println();
    }

    public List<String> tokenize(String text) {
        int length = text.length();
        String LowerText = text.toLowerCase();
        Boolean[][] D = new Boolean[length][length];
        List<List<String>> result = new ArrayList<>();
        List<String> path = new ArrayList<>();
        for (int l = 1; l <= length; l++){
            for (int i = 0; i <= length-l; i++){
                int j = i+l-1;
                D[i][j] = false;
                String subs = LowerText.substring(i, j+1);
                if (hm.containsKey(subs))
                    D[i][j] = true;
                else {
                    for (int k = i; k <= j-1; k++){
                        if (D[i][k] && D[k+1][j])
                            D[i][j] = true;
                    }
                }
            }
        }
        System.out.println(D[0][length-1]);
        if ( !D[0][length-1] )
            throw new UnsupportedOperationException("Porter Stemmer Unimplemented");
        wordBreakResult(LowerText, hm, 0, D, path, result);
        System.out.println(result);
        return new ArrayList<>(FinalResult(result, hm));
    }

    public void wordBreakResult(String text, HashMap hm, int start, Boolean[][]D, List<String> path, List<List<String>> result){
        int length = text.length();
        if (start == length){
            List<String> path1 = new ArrayList<>(path);
            path1.removeAll(StopWords.stopWords);
            result.add(path1);
            return;
        }
        if (!D[start][length-1]){
            return;
        }
        for (int j = start; j < text.length(); j++){
            String token = text.substring(start, j+1);
            if (!hm.containsKey(token)){
                continue;
            }
            path.add(token);
            wordBreakResult(text, hm, j+1, D, path, result);
            path.remove(path.size()-1);
        }
    }

    public List<String> FinalResult(List<List<String>> result, HashMap hm){
        List<Double> Prob = new ArrayList<>();
        for (List<String> li: result){
            Long freq = 0L;
            Double prob = 1.0;
            for (String token: li){
                Long tok = (Long) hm.get(token);
                freq = freq + tok;
            }
            for (String token: li){
                Long tok = (Long) hm.get(token);
                prob = prob * (tok*1.0 / freq);
            }
            Prob.add(prob);
        }
        Double max = Collections.max(Prob);
        System.out.println(result.get(Prob.indexOf(max)));
        return result.get(Prob.indexOf(max));
    }

}
