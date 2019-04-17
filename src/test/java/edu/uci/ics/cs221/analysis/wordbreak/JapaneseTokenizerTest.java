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
        List<String> expected = Arrays.asList("君", "が", "好き", "だ");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test2() {
        String text = "桜が咲いていますよ";
        List<String> expected = Arrays.asList("桜", "が", "咲いて", "います", "よ");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void test3() {
            String text = "花火スカイ電車インターネット";
            List<String> expected = Arrays.asList("花火", "スカイ", "電車", "インターネット");
            JapaneseTokenizer tokenizer = new JapaneseTokenizer();
            assertEquals(expected, tokenizer.tokenize(text));
    }


    @Test
    public void test4() {
        String text = "明日は花火大会がありますよ一緒に行きませんか";
        List<String> expected = Arrays.asList("明日", "は", "花火大会", "が", "あります", "よ", "一緒", "に", "行きません", "か");
        JapaneseTokenizer tokenizer = new JapaneseTokenizer();
        assertEquals(expected, tokenizer.tokenize(text));
    }

}
