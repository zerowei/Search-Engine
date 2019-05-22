package edu.uci.ics.cs221.index.inverted;

import java.util.List;

public class docListRID {
    String keyword;
    List<Integer> docIds;
    byte[] encodedPositionRIDs;

    docListRID(String keyword, List<Integer> docIds){
        this.keyword = keyword;
        this.docIds = docIds;
    }

    docListRID(String keyword, List<Integer> docIds, byte[] encodedPositionRIDs){
        this.keyword = keyword;
        this.docIds = docIds;
        this.encodedPositionRIDs = encodedPositionRIDs;
    }

    @Override public String toString() {
        return "docListRID{" +
                "keyword: " + keyword +
                ", docIds: " + docIds;
    }

}
