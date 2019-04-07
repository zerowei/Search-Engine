package edu.uci.ics.cs221.analysis;

import java.util.*;

/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 *
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 *
 * Requirements:
 *  - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 *  - White spaces and punctuations should be removed from the result tokens.
 *  - All tokens should be converted to lower case.
 *  - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 *
 */
public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public PunctuationTokenizer() {}

    public List<String> tokenize(String text) {
        NaiveAnalyzer analyzer = new NaiveAnalyzer();
        List<String> InitialToken = analyzer.analyze(text);
        System.out.println("Initial tokens after NaiveAnalyzer: " + InitialToken);
        List<String> tokens = new ArrayList<>();
        if (InitialToken.size() != 0){
            for (Object Initial : InitialToken) {
                String token = (String) Initial;
                for (Object obj : punctuations) {
                    String punctuation = (String) obj;
                    token = token.replace(punctuation, "");
                }
                tokens.add(token);
            }
        }
        else {
            throw new UnsupportedOperationException("Punctuation Tokenizer Unimplemented");
        }
        tokens.removeAll(StopWords.stopWords);
        System.out.println("Final tokens: " + tokens);
        return tokens;
    }
}
