package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.primitives.Bytes;
import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class manages an disk-based inverted index and all the documents in the inverted index.
 *
 * Please refer to the project 2 wiki page for implementation guidelines.
 */
public class InvertedIndexManager {

    /** File Structure
     *  Header File:
     *      Each index:
     *          4 Bytes     -> offset of the segment RID list, assume with value k
     *          4 Bytes     -> Length of the first keyword, assume with value n
     *          n Bytes     -> bytes of the word
     *          4 Bytes     -> Length of the second keyword, assume with value m
     *          m Bytes     -> bytes of the word
     *          ...
     *          kth Bytes   -> compressed list of RID list of the segments
     *
     *  Segment File:
     *      Each segment:
     *          4 Bytes         -> length of the compressed segment list, assume with value m
     *          Next m Bytes    -> compressed segment list
     *          4 Bytes         -> length of the compressed position RID list (absolute offset in bytes), assume with value n
     *          Next n Bytes    -> compressed position RID list
     *
     *   Position File:
     *       Each position:
     *          k Bytes         -> length of the compressed position list, assume with value m, note that the length itself is compressed as well
     *          Next m Bytes    -> compressed position list
     */

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the indexes.
     * <p>
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     * <p>
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    public static int PAGE_SIZE = 4096;

    private Analyzer analyzer;
    private String indexFolder;
    private Compressor compressor;
    public Map<Integer, Document> documents = new TreeMap<>();
    public Map<String, InvertedIndex> indexes = new TreeMap<>();
    public Integer numDocuments = 0;
    private Integer numSegments = 0;
    public Boolean flag = false;

    private String getDocumentStorePathString(int storeNum) {
        return indexFolder + "/docs_" + storeNum + ".db";
    }

    // Test cases fail if return Paths.get() directly here
    private String getHeaderFilePathString(int segmentNum) {
        return indexFolder + "/header_" + segmentNum + ".txt";
    }

    private String getSegmentFilePathString(int segmentNum) {
        return indexFolder + "/segment_" + segmentNum + ".txt";
    }

    private String getPositionFilePathString(int segmentNum) {
        return indexFolder + "/segment_position_" + segmentNum + ".txt";
    }

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        this.analyzer = analyzer;
        this.indexFolder = indexFolder;

        this.compressor = new NaiveCompressor();
    }

    private InvertedIndexManager(String indexFolder, Analyzer analyzer, Compressor compressor) {
        this.analyzer = analyzer;
        this.indexFolder = indexFolder;
        this.compressor = compressor;
        this.flag = true;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    /**
     * Creates a positional index with the given folder, analyzer, and the compressor.
     * Compressor must be used to compress the inverted lists and the position lists.
     *
     */
    public static InvertedIndexManager createOrOpenPositional(String indexFolder, Analyzer analyzer, Compressor compressor) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer, compressor);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer, compressor);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory indexes until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        Preconditions.checkNotNull(document);

        documents.put(numDocuments, document);
        List<String> tokens = analyzer.analyze(document.getText());
        for (int tokenPosition = 0; tokenPosition < tokens.size(); tokenPosition++) {
            String token = tokens.get(tokenPosition);
            List<Integer> documentIds;
            InvertedIndex index;

            if (indexes.containsKey(token)) {
                index = indexes.get(token);
                documentIds = new ArrayList<>(index.docPositions.keySet());
                if (documentIds.get(documentIds.size() - 1).equals(numDocuments) == false) {
                    index.docPositions.put(numDocuments, Arrays.asList(tokenPosition));
                }
                else {
                    List<Integer> positions = new ArrayList<>(index.docPositions.get(numDocuments));
                    positions.add(tokenPosition);
                    index.docPositions.put(numDocuments, positions);
                }
            } else {
                Map<Integer, List<Integer>> docPositions = new TreeMap<>();
                docPositions.put(numDocuments, Arrays.asList(tokenPosition));
                index = new InvertedIndex(token, docPositions);
                indexes.put(token, index);
            }
            //System.out.println(indexes);
        }

        numDocuments += 1;
        if (numDocuments == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }

    }

    class InvertedIndexWriter {
        AutoFlushBuffer headerFileBuffer;
        AutoFlushBuffer segmentFileBuffer;
        AutoFlushBuffer positionFileBuffer;
        int segmentNum;

        List<String> keywordList;
        List<Integer> segmentRidList;

        InvertedIndexWriter(int segmentNum) {
            this.segmentNum = segmentNum;

            PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(segmentNum)
                    ));
            PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(segmentNum)
                    ));
            PageFileChannel positionFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getPositionFilePathString(segmentNum)
                    ));
            this.headerFileBuffer = new AutoFlushBuffer(headerFileChannel);
            this.segmentFileBuffer = new AutoFlushBuffer(segmentFileChannel);
            this.positionFileBuffer = new AutoFlushBuffer(positionFileChannel);

            this.keywordList = new ArrayList<>();
            this.segmentRidList = new ArrayList<>();
        }

        void finish() {
            headerFileBuffer.putInt(keywordList.size());
            for (String keyword : keywordList) {
                byte[] bytes = keyword.getBytes();
                headerFileBuffer.putInt(bytes.length);
                headerFileBuffer.put(bytes);
            }

            byte[] encodedRIDs = compressor.encode(segmentRidList);
            headerFileBuffer.putInt(encodedRIDs.length);
            headerFileBuffer.put(encodedRIDs);

            headerFileBuffer.flush();
            segmentFileBuffer.flush();
            positionFileBuffer.flush();

            try {
                headerFileBuffer.close();
                segmentFileBuffer.close();
                positionFileBuffer.close();
            } catch (Exception e) {
                System.out.println("error when closing buffer");
            }
        }

        void writeOneRowToFileSequentially(InvertedIndex index) {
            // Write the header file after the entire writing phase of segment and position file finish
            // because it needs to compress all the RIDs together and cannot be written row by row
            keywordList.add(index.keyword);
            segmentRidList.add(segmentFileBuffer.getRID());

            assert index.docPositions.keySet().size() > 0;
            List<Integer> documentIds = new ArrayList<>(index.docPositions.keySet());
            byte[] encodedDocumentIds = compressor.encode(documentIds);
            segmentFileBuffer.putInt(encodedDocumentIds.length);
            segmentFileBuffer.put(encodedDocumentIds);

            List<Integer> positionOffsetList = new ArrayList<>();
            for (int i = 0; i < documentIds.size(); i++) {
                assert index.docPositions.get(documentIds.get(i)).size() > 0;

                positionOffsetList.add(positionFileBuffer.getRID());

                byte[] encodedPositions = compressor.encode(index.docPositions.get(documentIds.get(i)));
                positionFileBuffer.put(compressor.encode(Arrays.asList(encodedPositions.length)));
                positionFileBuffer.put(encodedPositions);
            }

            byte[] encodedPositionOffsetList = compressor.encode(positionOffsetList);
            segmentFileBuffer.putInt(encodedPositionOffsetList.length);
            segmentFileBuffer.put(encodedPositionOffsetList);
        }
    }


    /**
     * Flushes all the documents in the in-memory segment indexes to disk. If the indexes is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (indexes.isEmpty() && documents.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<Integer, Document>> itr = documents.entrySet().iterator();
        DocumentStore documentStore = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(numSegments), itr);
        documentStore.close();

        InvertedIndexWriter writer = new InvertedIndexWriter(numSegments);

        for (InvertedIndex index : indexes.values()) {
            writer.writeOneRowToFileSequentially(index);
        }

        writer.finish();

        numDocuments = 0;
        indexes.clear();
        documents.clear();

        numSegments += 1;

        if (numSegments == InvertedIndexManager.DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }
    }

    // Since the underlying file is based on pages, we need such an iterator to make life easier
    class InvertedIndexIterator implements Iterator<InvertedIndex>, AutoCloseable {
        PageFileChannel headerFile, segmentFile, positionFile;
        AutoLoadBuffer headerFileBuffer, segmentFileBuffer, positionFileBuffer;
        Compressor compressor;

        int currentKeywordId;
        List<String> keywordList;
        List<Integer> segmentRidList;

        InvertedIndexIterator(
                int segmentId,
                Compressor compressor
        ) {
            this.compressor = compressor;

            headerFile = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(segmentId)
                    ));
            segmentFile = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(segmentId)
                    ));
            positionFile = PageFileChannel.createOrOpen(
                    Paths.get(
                            getPositionFilePathString(segmentId)
                    ));
            headerFileBuffer = new AutoLoadBuffer(headerFile);
            segmentFileBuffer = new AutoLoadBuffer(segmentFile);
            positionFileBuffer = new AutoLoadBuffer(positionFile);

            this.keywordList = new ArrayList<>();
            this.segmentRidList = new ArrayList<>();

            currentKeywordId = 0;
            int numKeywords = headerFileBuffer.getInt();
            for (int i = 0; i < numKeywords; i++) {
                int lenKeyword = headerFileBuffer.getInt();
                byte[] keywordBytes = headerFileBuffer.getByteArray(lenKeyword);
                keywordList.add(new String(keywordBytes));
            }

            int lenEncodedRidList = headerFileBuffer.getInt();
            byte[] encodedRidList = headerFileBuffer.getByteArray(lenEncodedRidList);
            segmentRidList = compressor.decode(encodedRidList);
        }

        @Override public boolean hasNext() {
            return currentKeywordId < keywordList.size();
        }

        // Read indexes sequentially, that means read header file, segment file and position file sequentially
        @Override public InvertedIndex next() {
            if (hasNext() == false) {
                return null;
            }

            String keyword = keywordList.get(currentKeywordId);

            Map<Integer, List<Integer>> docPositions = new TreeMap<>();
            int lengthEncodedDocumentIds = segmentFileBuffer.getInt();
            byte[] encodedDocumentIds = segmentFileBuffer.getByteArray(lengthEncodedDocumentIds);
            int lengthPositionRIDs = segmentFileBuffer.getInt();
            byte[] encodedPositionRIDs = segmentFileBuffer.getByteArray(lengthPositionRIDs);

            List<Integer> documentIdList = compressor.decode(encodedDocumentIds);
            // Not used in sequential reading
            List<Integer> positionRidList = compressor.decode(encodedPositionRIDs);

            for (int documentId : documentIdList) {
                int lengthPosition;
                if (compressor instanceof NaiveCompressor) {
                    lengthPosition = positionFileBuffer.getInt();
                } else {
                    List<Byte> lengthPositionBytes = new ArrayList<>();
                    final byte mask = (byte) (1 << 7);
                    byte b = mask;
                    while ((b & mask) != 0) {
                        b = positionFileBuffer.getByte();
                        lengthPositionBytes.add(b);
                    }
                    lengthPosition = compressor.decode(Bytes.toArray(lengthPositionBytes)).get(0);
                }

                byte[] encodedPositions = positionFileBuffer.getByteArray(lengthPosition);
                List<Integer> positions = compressor.decode(encodedPositions);

                docPositions.put(documentId, positions);
            }

            currentKeywordId++;

            InvertedIndex result = new InvertedIndex(keyword, docPositions);
            //System.out.println(result);
            return result;
        }

        // Find the next index that matches the keyword
        public docListRID next(List<String> keywords, boolean includePosition) {

            // In case that the same keyword is searched multiple times, for example, "cat cat cat" is searched as a phrase,
            // then the iterator shouldn't move to the next keyword until search something rather than "cat"
            final docListRID nulldocListRID = new docListRID(null, new ArrayList<>(), new ArrayList<>());

            if (!hasNext() || keywords.isEmpty()) {
                return nulldocListRID;
            }

            int i;
            int documentIdPageId = 0;
            int documentIdOffset = 0;


            for (i = currentKeywordId; i < keywordList.size(); i++){
                if (!keywords.contains(keywordList.get(i))) {
                    continue;
                }
                currentKeywordId = i;
                Integer segmentRId = segmentRidList.get(i);
                documentIdPageId = segmentRId / PAGE_SIZE;
                documentIdOffset = segmentRId % PAGE_SIZE;
                break;
            }

            if (i == keywordList.size() && currentKeywordId == 0){
                return nulldocListRID;
            }

            segmentFileBuffer.setPageIdAndOffset(documentIdPageId, documentIdOffset);

            int lengthEncodedDocumentIds = segmentFileBuffer.getInt();
            byte[] encodedDocumentIds = segmentFileBuffer.getByteArray(lengthEncodedDocumentIds);
            int lengthPositionRIDs = segmentFileBuffer.getInt();
            byte[] encodedPositionRIDs = segmentFileBuffer.getByteArray(lengthPositionRIDs);

            List<Integer> documentIdList = compressor.decode(encodedDocumentIds);
            System.out.println(documentIdList);
            // Not used in sequential reading
            List<Integer> positionRidList = compressor.decode(encodedPositionRIDs);
            docListRID docRID;
            // Maybe we need to return positionRidList rather than the entire positionList due to memory limit?
            if (!includePosition){
                docRID = new docListRID(keywordList.get(currentKeywordId), documentIdList);
            }
            else {
                docRID = new docListRID(keywordList.get(currentKeywordId), documentIdList, positionRidList);
            }

            return docRID;

        }

        @Override public void close() throws Exception {
            try {
                headerFile.close();
                segmentFile.close();
                positionFile.close();
            } catch (Exception e) {
                throw new Exception("error when closing iterator");
            }
        }
    }


    private ByteBuffer listInt2ByteBuffer(List<Integer> list) {
        ByteBuffer result = ByteBuffer.allocate(list.size() * 4);
        for (Integer i : list) {
            result.putInt(i.intValue());
        }

        return result;
    }

    private void deleteAllFiles(int segNum) {
        List<String> fileNamesToDelete = Arrays.asList(
                getHeaderFilePathString(segNum),
                getSegmentFilePathString(segNum),
                getPositionFilePathString(segNum),
                getDocumentStorePathString(segNum)
        );

        for (String name: fileNamesToDelete) {
            File file = new File(name);
            if ( file.delete() ) {
                //System.out.println("Deleted " + name + " successfully");
            } else {
                //System.out.println("Failed to delete " + name);
            }
        }

        return;
    }

    private void renameAllFiles(int oldSegNum, int newSegNum) {
        Map<String, String> fileNamesToRename = new HashMap<String, String>() {{
            put(getHeaderFilePathString(oldSegNum), getHeaderFilePathString(newSegNum));
            put(getSegmentFilePathString(oldSegNum), getSegmentFilePathString(newSegNum));
            put(getPositionFilePathString(oldSegNum), getPositionFilePathString(newSegNum));
        }};

        for (Map.Entry<String,String> entry : fileNamesToRename.entrySet()) {
            File file = new File(entry.getKey());
            if ( file.renameTo(new File(entry.getValue()))) {
                //System.out.println("Rename " + entry.getKey() + " to " + entry.getValue() + " successfully");
            } else {
                //System.out.println("Fail to rename " + entry.getKey() + " to " + entry.getValue());
            }
        }

        // Do a full copy to avoid checksum failure of MapDB
        DocumentStore oldStore = MapdbDocStore.createOrOpen(getDocumentStorePathString(oldSegNum));
        DocumentStore newStore = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(newSegNum), oldStore.iterator());
        oldStore.close();
        newStore.close();

        File file = new File(getDocumentStorePathString(oldSegNum));
        if ( file.delete() ) {
            //System.out.println("Deleted " + getDocumentStorePathString(oldSegNum) + " successfully");
        } else {
            //System.out.println("Failed to delete " + getDocumentStorePathString(oldSegNum));
        }

        return;
    }

    class BulkLoadIterator implements Iterator<Map.Entry<Integer, Document>> {
        Iterator<Map.Entry<Integer, Document>> iteratorA, iteratorB;
        int numEntryA;

        public BulkLoadIterator(Iterator<Map.Entry<Integer, Document>> iteratorA, Iterator<Map.Entry<Integer, Document>> iteratorB, int numEntryA) {
            this.iteratorA = iteratorA;
            this.iteratorB = iteratorB;
            this.numEntryA = numEntryA;
        }

        @Override public boolean hasNext() {
            return iteratorA.hasNext() || iteratorB.hasNext();
        }

        @Override public Map.Entry<Integer, Document> next() {
            if (iteratorA.hasNext()) {
                return iteratorA.next();
            }
            Map.Entry<Integer, Document> tempEntry = iteratorB.next();
            return new AbstractMap.SimpleEntry<Integer, Document>(tempEntry.getKey() + numEntryA, tempEntry.getValue());
        }
    }

    private void mergeSegments(int segNumA, int segNumB, int segNumNew, Compressor compressor) {
        //System.out.println("------- merging " + segNumA + " and " + segNumB);

        // To avoid the document store name duplicated with other existing ones, we name it to a temp name
        // and then rename it after the old ones are deleted
        final int segNumTemp = 99999;

        InvertedIndexIterator iteratorA = new InvertedIndexIterator(segNumA, compressor);
        InvertedIndexIterator iteratorB = new InvertedIndexIterator(segNumB, compressor);

        DocumentStore documentStoreA = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumA));
        DocumentStore documentStoreB = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumB));

        DocumentStore documentStoreNew = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(segNumTemp), new BulkLoadIterator(documentStoreA.iterator(), documentStoreB.iterator(),
                (int) documentStoreA.size()));
        InvertedIndexWriter writerNew = new InvertedIndexWriter(segNumTemp);

        InvertedIndex indexA = null, indexB = null;
        if (iteratorA.hasNext()) {
            indexA = iteratorA.next();
        }
        if (iteratorB.hasNext()) {
            indexB = iteratorB.next();
        }

        while (indexA != null || indexB != null) {

            InvertedIndex indexNew = null;

            if (indexA != null && indexB != null && indexA.keyword.compareTo(indexB.keyword) == 0) {
                String keyword = indexA.keyword;

                Map<Integer, List<Integer>> docPositions = indexA.docPositions;
                Map<Integer, List<Integer>> docPositionsB = new TreeMap<>();
                for (Integer integer : indexB.docPositions.keySet()){
                    docPositionsB.put(integer + (int) documentStoreA.size(), indexB.docPositions.get(integer));
                }
                docPositions.putAll(docPositionsB);

                //System.out.println("\nindex A \t" + indexA.toString());
                //System.out.println("index B \t" + indexB.toString());

                indexNew = new InvertedIndex(keyword, docPositions);

                indexA = iteratorA.next();
                indexB = iteratorB.next();

            } else if ( indexA != null && (indexB == null || indexA.keyword.compareTo(indexB.keyword) < 0) ) {
                indexNew = indexA;

                //System.out.println("\nindex A \t" + indexA.toString());

                indexA = iteratorA.next();
            } else if (indexB != null && (indexA == null || indexA.keyword.compareTo(indexB.keyword) > 0)) {
                String keyword = indexB.keyword;
                Map<Integer, List<Integer>> docPositionsB = new TreeMap<>();
                for (Integer integer : indexB.docPositions.keySet()) {
                    docPositionsB.put(integer + (int) documentStoreA.size(), indexB.docPositions.get(integer));
                }
                indexNew = new InvertedIndex(keyword, docPositionsB);

                //System.out.println("\nindex B \t" + indexB.toString());

                indexB = iteratorB.next();
            } else {
                assert(false);
            }

            //System.out.println("new index\t" + indexNew.toString());

            writerNew.writeOneRowToFileSequentially(indexNew);
        }

        writerNew.finish();

        try {
            iteratorA.close();
            iteratorB.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        documentStoreA.close();
        documentStoreB.close();
        documentStoreNew.close();

        deleteAllFiles(segNumA);
        deleteAllFiles(segNumB);
        renameAllFiles(segNumTemp, segNumNew);

        return;
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);

        for (int i = 0; i < numSegments; i+=2) {
            mergeSegments(i, i+1, i/2, compressor);
        }
        numSegments /= 2;
    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
     *
     * @param keyword keyword, cannot be null.
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchQuery(String keyword) {
        Preconditions.checkNotNull(keyword);
        if (keyword.equals("")) {
            return Collections.emptyIterator();
        }

        List<Document> results = new ArrayList<>();
        keyword = analyzer.analyze(keyword).get(0);

        for (int i = 0; i < getNumSegments(); i++) {
            InvertedIndexIterator itr = new InvertedIndexIterator(i, compressor);
            Set<Integer> documentIds = new HashSet<>(itr.next(Arrays.asList(keyword), false).docIdList);

            if (documentIds.isEmpty()){
                return Collections.emptyIterator();
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));

            for (Integer documentId : documentIds){
                results.add(documentStore.getDocument(documentId));
            }

            documentStore.close();

            try {
                itr.close();
            }
            catch (Exception e){
                System.out.println("error when closing iterator");
            }

        }

        return results.iterator();
        //throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        if (keywords.equals(Arrays.asList(""))){
            return Collections.emptyIterator();
        }

        List<Document> results = new ArrayList<>();
        List<String> words = new ArrayList<>();

        for (String keyword : keywords){
            words.add(analyzer.analyze(keyword).get(0));
        }

        Collections.sort(words);
        int length = words.size();

        for (int i = 0; i < getNumSegments(); i++) {
            // System.out.println("----------Segment Num " + i);
            InvertedIndexIterator itr = new InvertedIndexIterator(i, compressor);
            List<String> copy = new ArrayList<>(words);

            Set<Integer> intersection = new HashSet<>(itr.next(copy, false).docIdList);

            if (intersection.isEmpty()){
                try {
                    itr.close();
                }
                catch (Exception e){
                    System.out.println("error when closing iterator");
                }
                continue;
            }

            copy.remove(0);

            for (int j = 1; j < length; j++){
                intersection.retainAll(itr.next(copy, false).docIdList);
                copy.remove(0);
                if (intersection.isEmpty()){
                    break;
                }
            }

            if (intersection.isEmpty()){
                try {
                    itr.close();
                }
                catch (Exception e){
                    System.out.println("error when closing iterator");
                }
                continue;
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));

            for (Integer e : intersection){
                results.add(documentStore.getDocument(e));
            }

            documentStore.close();
            copy.addAll(words);

            try {
                itr.close();
            }
            catch (Exception e){
                System.out.println("error when closing iterator");
            }

        }
        return results.iterator();
        // throw new UnsupportedOperationException();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        if (keywords.equals(Arrays.asList(""))){
            return Collections.emptyIterator();
        }

        if (keywords.isEmpty()){
            return Collections.emptyIterator();
        }

        List<Document> results = new ArrayList<>();
        List<String> words = new ArrayList<>();

        for (String keyword : keywords){
            words.add(analyzer.analyze(keyword).get(0));
        }

        Collections.sort(words);
        int length = words.size();
        List<String> copy = new ArrayList<>(words);

        for (int i = 0; i < getNumSegments(); i++) {
            InvertedIndexIterator itr = new InvertedIndexIterator(i, compressor);
            Set<Integer> union = new HashSet<>(itr.next(copy, false).docIdList);

            if (union.isEmpty()){
                try {
                    itr.close();
                }
                catch (Exception e){
                    System.out.println("error when closing iterator");
                }
                continue;
            }
            copy.remove(0);

            for (int j = 1; j < length; j++){
                union.addAll(itr.next(copy, false).docIdList);
                copy.remove(0);
            }

            if (union.isEmpty()){
                try {
                    itr.close();
                }
                catch (Exception e){
                    System.out.println("error when closing iterator");
                }
                continue;
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));
            for (Integer e : union){
                results.add(documentStore.getDocument(e));
            }

            documentStore.close();
            copy.addAll(words);

            try {
                itr.close();
            }
            catch (Exception e){
                System.out.println("error when closing iterator");
            }

        }

        return results.iterator();
        // throw new UnsupportedOperationException();
    }

    class DocumentIterator implements  Iterator<Document> {
        int currentDocumentStoreId;
        int currentDocumentId;
        DocumentStore currentDocumentStore;

        DocumentIterator() {
            currentDocumentStoreId = 0;
            currentDocumentId = 0;
            String docStorePath = getDocumentStorePathString(currentDocumentStoreId);
            currentDocumentStore = MapdbDocStore.createOrOpenReadOnly(docStorePath);
        }

        @Override public boolean hasNext() {
            return currentDocumentStoreId < numSegments;
        }

        @Override public Document next() {
            Document result = currentDocumentStore.getDocument(currentDocumentId);

            currentDocumentId++;
            if (currentDocumentId >= currentDocumentStore.size()) {
                currentDocumentStoreId++;
                currentDocumentStore.close();
                String docStorePath = getDocumentStorePathString(currentDocumentStoreId);
                if (currentDocumentStoreId < numSegments) {
                    currentDocumentStore = MapdbDocStore.createOrOpenReadOnly(docStorePath);
                }

                currentDocumentId = 0;
            }

            return result;
        }
    }

    /**
     * Performs a phrase search on a positional index.
     * Phrase search means the document must contain the consecutive sequence of keywords in exact order.
     *
     * You could assume the analyzer won't convert each keyword into multiple tokens.
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param phrase, a consecutive sequence of keywords
     * @return a iterator of documents matching the query
     */

    public Iterator<Document> searchPhraseQuery(List<String> phrase) {

        Preconditions.checkNotNull(phrase);

        if (!flag){
            throw new UnsupportedOperationException();
        }

        if (phrase.equals(Arrays.asList(""))){
            return Collections.emptyIterator();
        }
        if (phrase.isEmpty()){
            return Collections.emptyIterator();
        }

        Map<String, Integer> OrderdPhrase = new TreeMap<>();
        List<Document> finalResults = new ArrayList<>();
        int order = 0;

        for (int i = 0; i < phrase.size(); i++){
            if (analyzer.analyze(phrase.get(i)).isEmpty()){
                continue;
            }
            String token = analyzer.analyze(phrase.get(i)).get(0);
            OrderdPhrase.put(token, order);
            order++;
        }

        int length = OrderdPhrase.size();

        for (int segmentId = 0; segmentId < numSegments; segmentId++) {
            InvertedIndexIterator itr = new InvertedIndexIterator(segmentId, compressor);

            List<docListRID> totalListRID = new ArrayList<>();
            List<String> tokens = new ArrayList<>(OrderdPhrase.keySet());

            for (int k = 0; k < length; k++){
                docListRID docRID = itr.next(tokens, true);
                if (docRID.docIdList.isEmpty()){
                    // If the iterator is implemented correctly,
                    // then this branch means one or more keywords are not found in the current segment, right?
                    // Should we return a null iterator after closing all the files here?
                    break;
                }
                totalListRID.add(docRID);
                tokens.remove(0);
            }

            tokens.addAll(OrderdPhrase.keySet());
            try {
                itr.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (totalListRID.size() != tokens.size()){
                continue;
            }

            PageFileChannel positionFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getPositionFilePathString(segmentId)
                    ));
            AutoLoadBuffer positionFileBuffer = new AutoLoadBuffer(positionFileChannel);
            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segmentId));

            for (int l = 0; l < totalListRID.get(0).docIdList.size(); l++){
                Integer docID4firstWord = totalListRID.get(0).docIdList.get(l);

                int positionRID = totalListRID.get(0).positionRidList.get(l);

                int pageID = positionRID / PAGE_SIZE;
                int offset = positionRID % PAGE_SIZE;
                positionFileBuffer.setPageIdAndOffset(pageID, offset);

                int lengthPosition;
                final byte mask = (byte) (1 << 7);
                byte b = mask;

                if (compressor instanceof NaiveCompressor) {
                    lengthPosition = positionFileBuffer.getInt();
                } else {
                    List<Byte> lengthPositionBytes = new ArrayList<>();
                    while ((b & mask) != 0) {
                        b = positionFileBuffer.getByte();
                        lengthPositionBytes.add(b);
                    }
                    lengthPosition = compressor.decode(Bytes.toArray(lengthPositionBytes)).get(0);
                }

                byte[] encodedPositions = positionFileBuffer.getByteArray(lengthPosition);
                List<Integer> intersection = compressor.decode(encodedPositions);

                if (intersection.isEmpty()){
                    continue;
                }

                int record = 1;

                for (int m = 1; m < totalListRID.size(); m++){
                    if (!totalListRID.get(m).docIdList.contains(docID4firstWord)){
                        break;
                    }

                    record++;
                    int index = totalListRID.get(m).docIdList.indexOf(docID4firstWord);
                    int decodedRID = totalListRID.get(m).positionRidList.get(index);

                    int pageID4Others = decodedRID / PAGE_SIZE;
                    int offset4Others = decodedRID % PAGE_SIZE;
                    positionFileBuffer.setPageIdAndOffset(pageID4Others, offset4Others);

                    int lengthPosition4Others;
                    b = mask;
                    if (compressor instanceof NaiveCompressor) {
                        lengthPosition4Others = positionFileBuffer.getInt();
                    }
                    else {
                        List<Byte> lengthPositionBytes4Others = new ArrayList<>();
                        while ((b & mask) != 0) {
                            b = positionFileBuffer.getByte();
                            lengthPositionBytes4Others.add(b);
                        }
                        lengthPosition4Others = compressor.decode(Bytes.toArray(lengthPositionBytes4Others)).get(0);
                    }

                    byte[] encodedPositions4Others = positionFileBuffer.getByteArray(lengthPosition4Others);

                    int subtract = OrderdPhrase.get(tokens.get(m)) - OrderdPhrase.get(tokens.get(m-1));
                    for (int p = 0; p < intersection.size(); p++){
                        int newPosition = intersection.get(p)+subtract;
                        intersection.set(p, newPosition);
                    }

                    intersection.retainAll(compressor.decode(encodedPositions4Others));
                    if (intersection.isEmpty()){
                        break;
                    }
                }

                if (record == totalListRID.size() && !intersection.isEmpty()){
                    finalResults.add(documentStore.getDocument(docID4firstWord));
                }
            }

            documentStore.close();
            positionFileChannel.close();
        }

        return finalResults.iterator();
    }

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        return new DocumentIterator();
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     *
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     */
    public int getNumSegments() {
        return numSegments;
        //throw new UnsupportedOperationException();
    }

    class PositionalIndexSegmentForTestInternal {
        Map<String, List<Integer>> invertedLists;
        Map<Integer, Document> documents;
        Table<String, Integer, List<Integer>> positions;

        public PositionalIndexSegmentForTestInternal(Map<String, List<Integer>> invertedLists, Map<Integer, Document> documents, Table<String, Integer, List<Integer>> positions) {
            this.invertedLists = invertedLists;
            this.documents = documents;
            this.positions = positions;
        }
    }

    private PositionalIndexSegmentForTestInternal getIndexSegmentPositionalInternal(int segmentNum) {
        if (segmentNum >= numSegments)
            return null;

        // A local and temp variable, different from the global one
        Map<Integer, Document> documents = new TreeMap<>();
        DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segmentNum));
        Iterator<Map.Entry<Integer, Document>> it = documentStore.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Document> entry = it.next();
            documents.put(entry.getKey(), entry.getValue());
        }
        documentStore.close();

        Map<String, List<Integer>> invertedLists = new TreeMap<>();

        InvertedIndexIterator indexIterator = new InvertedIndexIterator(segmentNum, compressor);
        Table<String, Integer, List<Integer>> positions = HashBasedTable.create();

        while (indexIterator.hasNext()) {
            InvertedIndex index = indexIterator.next();
            List<Integer> docIds = new ArrayList<>(index.docPositions.keySet());
            invertedLists.put(index.keyword, docIds);

            for (int i = 0; i < docIds.size(); i++) {
                Integer docId = docIds.get(i);
                positions.put(index.keyword, docId, index.docPositions.get(docId));
            }
        }

        try {
            indexIterator.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new PositionalIndexSegmentForTestInternal(invertedLists, documents, positions);
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        PositionalIndexSegmentForTestInternal internal = getIndexSegmentPositionalInternal(segmentNum);
        if (internal == null) {
            return null;
        }
        return new InvertedIndexSegmentForTest(internal.invertedLists, internal.documents);
    }

    /**
     * Reads a disk segment of a positional index into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
        PositionalIndexSegmentForTestInternal internal = getIndexSegmentPositionalInternal(segmentNum);
        if (internal == null) {
            return null;
        }
        return new PositionalIndexSegmentForTest(internal.invertedLists, internal.documents, internal.positions);
    }

}
