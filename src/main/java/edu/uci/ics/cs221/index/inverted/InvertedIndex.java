package edu.uci.ics.cs221.index.inverted;

import java.util.List;

public class InvertedIndex {
    String keyword;
    List<Integer> documentIds;
    List<List<Integer>> positions;

    InvertedIndex(String keyword, List<Integer> documentIds, List<List<Integer>> positions) {
        this.keyword = keyword;
        this.documentIds = documentIds;
        this.positions = positions;
    }

    @Override public String toString() {
        assert positions.size() == documentIds.size();
        for (int i = 0; i < positions.size(); i++) {
            assert positions.get(i).size() > 0;
        }

        return "InvertedIndex{" +
                "keyword: " + keyword +
                ", documentIds: " + documentIds +
                " positions: " + positions +
                "}";
    }
}

