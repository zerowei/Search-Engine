package edu.uci.ics.cs221.analysis.wordbreak;
import edu.uci.ics.cs221.analysis.JapaneseTokenizer;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JapaneseTokenizerTest {
    @Test
    public void test1() {
        String text = "君が好きだ";
        List<String> expected = Arrays.asList("君", "好き");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2() {
        String text = "日本語のほうが簡単だと思います";
        List<String> expected = Arrays.asList("日本語", "の", "ほう", "簡単", "思い");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3() {
            String text = "花火炎熱インターネットスカイ";
            List<String> expected = Arrays.asList("花火", "炎熱", "インターネット", "スカイ");
            JapaneseTokenizer tokenizer = new JapaneseTokenizer();
            assertEquals(expected, tokenizer.tokenize(text));
    }


    @Test
    public void test4() {
        String text = "明日は花火大会がありますよ一緒に行きませんか";
        List<String> expected = Arrays.asList("明日", "花火大会", "あります", "一緒", "行きません");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

}
