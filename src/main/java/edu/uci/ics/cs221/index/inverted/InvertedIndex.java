package edu.uci.ics.cs221.index.inverted;

import java.util.List;
import java.util.Map;

public class InvertedIndex {
    String keyword;
    Map<Integer, List<Integer>> docPositions;

    InvertedIndex(String keyword, Map<Integer, List<Integer>> docPositions) {
        this.keyword = keyword;
        this.docPositions = docPositions;
    }

    @Override public String toString() {
        for (List<Integer> positions : docPositions.values()) {
            assert positions.size() > 0;
        }

        return "InvertedIndex{" +
                "keyword: " + keyword +
                ", documentIds: " + docPositions.keySet() +
                " positions: " + docPositions.values() +
                "}";
    }
}