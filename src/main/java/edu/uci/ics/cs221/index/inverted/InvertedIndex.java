package edu.uci.ics.cs221.index.inverted;

import java.util.Iterator;
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
        System.out.println(docPositions);
        Iterator<List<Integer>> itr = docPositions.values().iterator();
        while (itr.hasNext()){
            assert itr.next().size() > 0;
        }

        return "InvertedIndex{" +
                "keyword: " + keyword +
                ", documentIds: " + docPositions.keySet() +
                " positions: " + docPositions.values() +
                "}";
    }
}

