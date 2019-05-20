package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import org.checkerframework.checker.units.qual.A;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
     *          4 Bytes -> number of bytes (n) of the word
     *          n Bytes -> bytes of the word
     *          4 Bytes -> page id of the segment in the segment file // Not compressed
     *          4 Bytes -> offset of the segment in the segment file // Not compressed
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
     *          4 Bytes         -> length of the compressed position list, assume with value m
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

    private void writeOneRowToFileSequentially(InvertedIndex index,
                                               AutoFlushBuffer headerFileBuffer,
                                               AutoFlushBuffer segmentFileBuffer,
                                               AutoFlushBuffer positionFileBuffer ) {

        byte[] keywordBytes = index.keyword.getBytes();
        headerFileBuffer.putInt(keywordBytes.length);
        headerFileBuffer.put(keywordBytes);
        headerFileBuffer.putInt(segmentFileBuffer.getPageId());
        headerFileBuffer.putInt(segmentFileBuffer.getOffset());

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
            positionFileBuffer.putInt(encodedPositions.length);
            positionFileBuffer.put(encodedPositions);
        }

        byte[] encodedPositionOffsetList = compressor.encode(positionOffsetList);
        segmentFileBuffer.putInt(encodedPositionOffsetList.length);
        segmentFileBuffer.put(encodedPositionOffsetList);
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

        PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                Paths.get(
                        getHeaderFilePathString(numSegments)
                ));
        PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                Paths.get(
                        getSegmentFilePathString(numSegments)
                ));
        PageFileChannel positionFileChannel = PageFileChannel.createOrOpen(
                Paths.get(
                        getPositionFilePathString(numSegments)
                ));
        AutoFlushBuffer headerFileBuffer = new AutoFlushBuffer(headerFileChannel);
        AutoFlushBuffer segmentFileBuffer = new AutoFlushBuffer(segmentFileChannel);
        AutoFlushBuffer positionFileBuffer = new AutoFlushBuffer(positionFileChannel);

        for (Map.Entry<String, InvertedIndex> it : indexes.entrySet()) {
            writeOneRowToFileSequentially(it.getValue(), headerFileBuffer, segmentFileBuffer, positionFileBuffer);
        }

        headerFileBuffer.flush();
        segmentFileBuffer.flush();
        positionFileBuffer.flush();

        headerFileChannel.close();
        segmentFileChannel.close();
        positionFileChannel.close();

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
        // In case that the same keyword is searched multiple times, for example, "cat cat cat" is searched as a phrase,
        // then the iterator shouldn't move to the next keyword until search something rather than "cat"
        String currentKeyword;

        InvertedIndexIterator(
                PageFileChannel headerFile,
                PageFileChannel segmentFile,
                PageFileChannel positionFile,
                Compressor compressor
        ) {
            this.compressor = compressor;

            this.headerFile = headerFile;
            this.segmentFile = segmentFile;
            this.positionFile = positionFile;

            headerFileBuffer = new AutoLoadBuffer(headerFile);
            segmentFileBuffer = new AutoLoadBuffer(segmentFile);
            positionFileBuffer = new AutoLoadBuffer(positionFile);

            currentKeyword = null;
        }

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

            currentKeyword = null;
        }


        @Override public boolean hasNext() {
            return headerFileBuffer.hasRemaining();
        }

        // ToDo: cache the current page to avoid multiple read on the same page
        private List<Integer> getSegmentList(int pageId, int offset, int numOccurrence) {
            //System.out.println("get segment list; pageId: " + pageId + " offset: " + offset + " numOccu: " + numOccurrence);
            List<Integer> result = new ArrayList<>(numOccurrence);
            int numPages = (int) Math.ceil((numOccurrence * 4 + offset + 0.0) / PAGE_SIZE);

            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE * numPages);
            for (int i = pageId; i < pageId + numPages; i++) {
                buffer.put(segmentFile.readPage(i));
            }

            buffer.position(offset);
            for (int i = 0; i < numOccurrence; i++) {
                result.add(buffer.getInt());
            }

            return result;
        }

        // Read indexes sequentially, that means read header file, segment file and position file sequentially
        @Override public InvertedIndex next() {
            if (hasNext() == false) {
                return null;
            }

            int lenWords = headerFileBuffer.getInt();
            byte[] wordBytes = new byte[lenWords];
            for (int i = 0; i < lenWords; i++) {
                wordBytes[i] = headerFileBuffer.getByte();
            }
            String keyword = new String(wordBytes);
            // Skip pageId and offset of segment file when reading files sequentially
            headerFileBuffer.getInt();
            headerFileBuffer.getInt();

            Map<Integer, List<Integer>> docPositions = new TreeMap<>();
            int lengthEncodedDocumentIds = segmentFileBuffer.getInt();
            byte[] encodedDocumentIds = segmentFileBuffer.getByteArray(lengthEncodedDocumentIds);
            int lengthPositionRIDs = segmentFileBuffer.getInt();
            byte[] encodedPositionRIDs = segmentFileBuffer.getByteArray(lengthPositionRIDs);

            List<Integer> documentIdList = compressor.decode(encodedDocumentIds);
            // Not used in sequential reading
            List<Integer> positionRidList = compressor.decode(encodedPositionRIDs);

            for (int documentId : documentIdList) {
                int lengthPosition = positionFileBuffer.getInt();
                byte[] encodedPositions = positionFileBuffer.getByteArray(lengthPosition);
                List<Integer> positions = compressor.decode(encodedPositions);

                docPositions.put(documentId, positions);
            }

            InvertedIndex result = new InvertedIndex(keyword, docPositions);
            //System.out.println(result);
            return result;
        }

        // Find the next index that matches the keyword
        public InvertedIndex next(String keyword) {
            final InvertedIndex nullInvertedIndex = new InvertedIndex(null,new TreeMap<>());

            if (hasNext() == false || keyword == null) {
                return nullInvertedIndex;
            }

            int documentIdPageId = 0;
            int documentIdOffset = 0;

            while (hasNext() && keyword.equals(currentKeyword) == false) {
                int lenWords = headerFileBuffer.getInt();
                byte[] wordBytes = new byte[lenWords];
                for (int i = 0; i < lenWords; i++) {
                    wordBytes[i] = headerFileBuffer.getByte();
                }
                currentKeyword = new String(wordBytes);
                documentIdPageId = headerFileBuffer.getInt();
                documentIdOffset = headerFileBuffer.getInt();
            }
            if (keyword.equals(currentKeyword) == false) {
                return nullInvertedIndex;
            }
            segmentFileBuffer.setPageIdAndOffset(documentIdPageId, documentIdOffset);

            Map<Integer, List<Integer>> docPositions = new TreeMap<>();
            int lengthEncodedDocumentIds = segmentFileBuffer.getInt();
            byte[] encodedDocumentIds = segmentFileBuffer.getByteArray(lengthEncodedDocumentIds);
            int lengthPositionRIDs = segmentFileBuffer.getInt();
            byte[] encodedPositionRIDs = segmentFileBuffer.getByteArray(lengthPositionRIDs);

            List<Integer> documentIdList = compressor.decode(encodedDocumentIds);
            // Not used in sequential reading
            List<Integer> positionRidList = compressor.decode(encodedPositionRIDs);

            // Maybe we need to return positionRidList rather than the entire positionList due to memory limit?

            positionFileBuffer.setRID(positionRidList.get(0));
            for (int documentId : documentIdList) {
                int lengthPosition = positionFileBuffer.getInt();
                byte[] encodedPositions = positionFileBuffer.getByteArray(lengthPosition);
                List<Integer> positions = compressor.decode(encodedPositions);

                docPositions.put(documentId, positions);
            }

            InvertedIndex result = new InvertedIndex(currentKeyword, docPositions);
            return result;
        }

        // Find the next index that matches the keywords
        public InvertedIndex next(List<String> keywords) {
            final InvertedIndex nullInvertedIndex = new InvertedIndex(null, new TreeMap<>());

            if (hasNext() == false || keywords.isEmpty()) {
                return nullInvertedIndex;
            }

            int documentIdPageId = 0;
            int documentIdOffset = 0;

            while (hasNext()) {
                int lenWords = headerFileBuffer.getInt();
                byte[] wordBytes = new byte[lenWords];
                for (int i = 0; i < lenWords; i++) {
                    wordBytes[i] = headerFileBuffer.getByte();
                }
                currentKeyword = new String(wordBytes);

                documentIdPageId = headerFileBuffer.getInt();
                documentIdOffset = headerFileBuffer.getInt();
                if (!keywords.contains(currentKeyword)) {
                    continue;
                }

                segmentFileBuffer.setPageIdAndOffset(documentIdPageId, documentIdOffset);

                Map<Integer, List<Integer>> docPositions = new TreeMap<>();
                int lengthEncodedDocumentIds = segmentFileBuffer.getInt();
                byte[] encodedDocumentIds = segmentFileBuffer.getByteArray(lengthEncodedDocumentIds);
                int lengthPositionRIDs = segmentFileBuffer.getInt();
                byte[] encodedPositionRIDs = segmentFileBuffer.getByteArray(lengthPositionRIDs);

                List<Integer> documentIdList = compressor.decode(encodedDocumentIds);

                // Not used in sequential reading
                List<Integer> positionRidList = compressor.decode(encodedPositionRIDs);

                // Maybe we need to return positionRidList rather than the entire positionList due to memory limit?

                positionFileBuffer.setRID(positionRidList.get(0));
                for (int documentId : documentIdList) {
                    int lengthPosition = positionFileBuffer.getInt();
                    byte[] encodedPositions = positionFileBuffer.getByteArray(lengthPosition);
                    List<Integer> positions = compressor.decode(encodedPositions);

                    docPositions.put(documentId, positions);
                }

                InvertedIndex result = new InvertedIndex(currentKeyword, docPositions);

                return result;
            }

            return nullInvertedIndex;

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
        // System.out.println("------- merging " + segNumA + " and " + segNumB);

        // To avoid the document store name duplicated with other existing ones, we name it to a temp name
        // and then rename it after the old ones are deleted
        final int segNumTemp = 99999;

        PageFileChannel fileHeaderNew   = PageFileChannel.createOrOpen(Paths.get(getHeaderFilePathString(segNumTemp)));
        PageFileChannel fileSegmentNew    = PageFileChannel.createOrOpen(Paths.get(getSegmentFilePathString(segNumTemp)));
        PageFileChannel filePositionNew   = PageFileChannel.createOrOpen(Paths.get(getPositionFilePathString(segNumTemp)));

        AutoFlushBuffer bufferHeaderFileNew = new AutoFlushBuffer(fileHeaderNew);
        AutoFlushBuffer bufferSegmentFileNew = new AutoFlushBuffer(fileSegmentNew);
        AutoFlushBuffer bufferPositionFileNew = new AutoFlushBuffer(filePositionNew);

        InvertedIndexIterator iteratorA = new InvertedIndexIterator(segNumA, compressor);
        InvertedIndexIterator iteratorB = new InvertedIndexIterator(segNumB, compressor);

        DocumentStore documentStoreA = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumA));
        DocumentStore documentStoreB = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumB));

        DocumentStore documentStoreNew = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(segNumTemp), new BulkLoadIterator(documentStoreA.iterator(), documentStoreB.iterator(),
                (int) documentStoreA.size()));

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

            writeOneRowToFileSequentially(indexNew, bufferHeaderFileNew, bufferSegmentFileNew, bufferPositionFileNew);
        }

        bufferHeaderFileNew.flush();
        bufferSegmentFileNew.flush();
        bufferPositionFileNew.flush();

        try {
            iteratorA.close();
            iteratorB.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        fileHeaderNew.close();
        fileSegmentNew.close();
        filePositionNew.close();

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
            Set<Integer> documentIds = itr.next(keyword).docPositions.keySet();
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

            Set<Integer> intersection = itr.next(copy.get(0)).docPositions.keySet();

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
                intersection.retainAll(itr.next(copy.get(0)).docPositions.keySet());
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
            Set<Integer> union = new HashSet<>(itr.next(copy).docPositions.keySet());

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
                union.addAll(itr.next(copy).docPositions.keySet());
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

        for (int j = 0; j < numSegments; j++){
            PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(j)
                    ));
            PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(j)
                    ));
            PageFileChannel positionFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getPositionFilePathString(j)
                    ));

            InvertedIndexIterator itr = new InvertedIndexIterator(
                    headerFileChannel, segmentFileChannel, positionFileChannel, compressor);

            List<String> tokens = new ArrayList<>(OrderdPhrase.keySet());
            String firstKeyword = tokens.get(0);
            Set<Integer> intersection = new HashSet<>(itr.next(firstKeyword).docPositions.keySet());

            if (intersection.isEmpty()){
                headerFileChannel.close();
                segmentFileChannel.close();
                positionFileChannel.close();
                continue;
            }

            for (int k = 1; k < tokens.size(); k++){
                intersection.retainAll(itr.next(tokens.get(k)).docPositions.keySet());
                if (intersection.isEmpty()){
                    break;
                }
            }
            if (intersection.isEmpty()){
                headerFileChannel.close();
                segmentFileChannel.close();
                positionFileChannel.close();
                continue;
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(j));

            for (Integer integer: intersection){
                InvertedIndexIterator newItr = new InvertedIndexIterator(
                        headerFileChannel, segmentFileChannel, positionFileChannel, compressor);
                List<Integer> position4Firstword = newItr.next(firstKeyword).docPositions.get(integer);

                for (int l = 1; l < tokens.size(); l++){
                    int subtract = OrderdPhrase.get(tokens.get(l)) - OrderdPhrase.get(tokens.get(l-1));

                    for (int m = 0; m < position4Firstword.size(); m++){
                        int newPosition = position4Firstword.get(m)+subtract;
                        position4Firstword.set(m, newPosition);
                    }

                    position4Firstword.retainAll(newItr.next(tokens.get(l)).docPositions.get(integer));


                }
                if (!position4Firstword.isEmpty()){
                    finalResults.add(documentStore.getDocument(integer));
                }
            }
            documentStore.close();
            headerFileChannel.close();
            segmentFileChannel.close();
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
