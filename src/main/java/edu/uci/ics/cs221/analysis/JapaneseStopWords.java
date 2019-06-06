package edu.uci.ics.cs221.analysis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A list of stop words.
 * Please use this list and don't change it for uniform behavior in testing.
 */

public class JapaneseStopWords {

    public static Set<String> JapanesestopWords = new HashSet<>();

    static {
        JapanesestopWords.addAll(Arrays.asList(
                "と",
                "》",
                "《",
                "が",
                "だ",
                "て",
                "を",
                "は",
                "に",
                "た",
                "。",
                "」",
                "「",
                "…",
                "も",
                "で",
                "か",
                "ん",
                "う",
                "その",
                "？",
                "ます",
                "です",
                "！",
                "よ",
                "お",
                "｜",
                "この",
                "――",
                "ぬ",
                "な",
                "ね",
                "まで",
                "だけ",
                "じゃ",
                "って",
                "ながら",
                "という",
                "でも",
                "し",
                "など",
                "けど",
                "たり",
                "らしい",
                "じ"
        ));
    }
}
