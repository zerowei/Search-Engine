package edu.uci.ics.cs221.index.inverted;

import java.util.List;

public class docListRID {
    String keyword;
    List<Integer> docIdList;
    List<Integer> positionRidList;

    docListRID(String keyword, List<Integer> docIds){
        this.keyword = keyword;
        this.docIdList = docIds;
    }

    docListRID(String keyword, List<Integer> docIdList, List<Integer> positionRidList){
        this.keyword = keyword;
        this.docIdList = docIdList;
        this.positionRidList = positionRidList;
    }

    @Override public String toString() {
        return "docListRID{" +
                "keyword: " + keyword +
                ", docIds: " + docIdList +
                ", positionRIDs: " + positionRidList;
    }

}
