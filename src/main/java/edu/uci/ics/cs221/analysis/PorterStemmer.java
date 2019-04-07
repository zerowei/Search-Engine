package edu.uci.ics.cs221.analysis;

/**
 * Project 1, task 3: Incorporate a Porter stemmer.
 *
 * Stemming is the process of reducing a word into its "stem" ("root") form.
 *
 * Porter stemming is a classic and popular algorithm that uses a set of rules and steps to process a token.
 * We ask you to incorporate the following existing Porter stemmer implementation into this project.
 *
 * https://github.com/apache/lucene-solr/blob/master/lucene/analysis/common/src/java/org/apache/lucene/analysis/en/PorterStemmer.java
 *
 */
public class PorterStemmer implements Stemmer {

    public PorterStemmer() {}

    public String stem(String token) {
        throw new UnsupportedOperationException("Porter Stemmer Unimplemented");
    }

}
