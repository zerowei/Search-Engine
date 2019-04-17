package edu.uci.ics.cs221.analysis;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JapaneseTokenizer {
    public List<String> Words = new ArrayList<>();
    public List<String> result = new ArrayList<>();
    public static Set<String> punctuations = new HashSet<>();
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public JapaneseTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("JapaneseDic.txt");
            List<String> dictLines = Files.readAllLines(Paths.get(dictResource.toURI()), Charset.forName("GBK"));
            for (String dict: dictLines){
                List<String> japanWords = Arrays.asList(dict.split("\\s+"));
                Words.add(japanWords.get(1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> tokenize(String text) {
        int length = text.length();
        if (length == 0) {
            return new ArrayList<>();
        }
        for (String str: punctuations) {
            if (text.contains(str))
                throw new RuntimeException("Punctuations should not be input to JapaneseTokenizer");
        }
        wordBreakResult(0, text, Words, result);
        System.out.println(result);
        return result;
    }

    public void wordBreakResult(int start, String text, List<String> Words, List<String> result){
        int len = text.length();
        if (start == len)
            return;
        int index = 0;
        List<String> path = new ArrayList<>();

        for (int i = start; i< len; i++){
            String substr = text.substring(start, i+1);
            if (!Words.contains(substr)){
                continue;
            }
            index = i+1;
            path.add(substr);
        }
        int size = path.size();
        result.add(path.get(size-1));
        wordBreakResult(index, text, Words, result);
    }

}
