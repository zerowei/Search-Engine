package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
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
     *          4 Bytes -> page id of the segment in the segment file
     *          4 Bytes -> offset of the segment in the segment file
     *
     *  Segment File:
     *      Each segment:
     *          4 Bytes -> number of segment/occurrence of the word
     *          First Document:
     *              4 Bytes -> the id of the first document that contains the word
     *              4 Bytes -> page id of the position of the word in the Position File
     *              4 Bytes -> offset of the position of the word in the Position File
     *          Second Document:
     *              4 Bytes -> the id of the second document that contains the word
     *          ...
     *
     *   Position File:
     *       Each position:
     *          4 Bytes -> number of the positions of the word in the Position File
     *          4 Bytes -> the first position of the word in the related document
     *          4 Bytes -> the second position of the word in the related document
     *          ...
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

        //System.out.println("writing " + index.toString());

        byte[] keywordBytes = index.keyword.getBytes();
        headerFileBuffer.putInt(keywordBytes.length);
        headerFileBuffer.put(keywordBytes);
        headerFileBuffer.putInt(segmentFileBuffer.getPageId());
        headerFileBuffer.putInt(segmentFileBuffer.getOffset());

        assert index.docPositions.keySet().size() > 0;
        segmentFileBuffer.putInt(index.docPositions.keySet().size());
        List<Integer> documentIds = new ArrayList<>(index.docPositions.keySet());
        for (int i = 0; i < documentIds.size(); i++) {
            segmentFileBuffer.putInt(documentIds.get(i));
            segmentFileBuffer.putInt(positionFileBuffer.getPageId());
            segmentFileBuffer.putInt(positionFileBuffer.getOffset());

            assert index.docPositions.get(documentIds.get(i)).size() > 0;
            positionFileBuffer.putInt(index.docPositions.get(documentIds.get(i)).size());
            for (int position : index.docPositions.get(documentIds.get(i))) {
                positionFileBuffer.putInt(position);
            }
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
    class InvertedIndexIterator implements Iterator<InvertedIndex> {
        PageFileChannel headerFile, segmentFile, positionFile;
        AutoLoadBuffer headerFileBuffer, segmentFileBuffer, positionFileBuffer;

        InvertedIndexIterator(PageFileChannel headerFile,
                              PageFileChannel segmentFile,
                              PageFileChannel positionFile) {
            this.headerFile = headerFile;
            this.segmentFile = segmentFile;
            this.positionFile = positionFile;

            headerFileBuffer = new AutoLoadBuffer(headerFile);
            segmentFileBuffer = new AutoLoadBuffer(segmentFile);
            positionFileBuffer = new AutoLoadBuffer(positionFile);
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
            int numDocuments = segmentFileBuffer.getInt();
            for (int i = 0; i < numDocuments; i++) {
                Integer documentId = segmentFileBuffer.getInt();
                // Skip pageId and offset of position file when reading files sequentially
                segmentFileBuffer.getInt();
                segmentFileBuffer.getInt();

                List<Integer> currentSegmentPositions = new ArrayList<>();
                int numCurrentSegmentPositions = positionFileBuffer.getInt();
                assert numCurrentSegmentPositions > 0;
                for (int j = 0; j < numCurrentSegmentPositions; j++) {
                    currentSegmentPositions.add(positionFileBuffer.getInt());
                }
                docPositions.put(documentId, currentSegmentPositions);
            }

            InvertedIndex result = new InvertedIndex(keyword, docPositions);
            //System.out.println(result);
            return result;
        }

        // Find the next index that matches the keyword
        public InvertedIndex next(String keyword) {

            while (hasNext()) {
                InvertedIndex result = next();
                //System.out.println(result);
                if (result.keyword.equals(keyword)) {
                    return result;
                }
            }
            return new InvertedIndex(new String(), new TreeMap<>());
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

    private void mergeSegments(int segNumA, int segNumB, int segNumNew) {
        //System.out.println("------- merging " + segNumA + " and " + segNumB);

        // To avoid the document store name duplicated with other existing ones, we name it to a temp name
        // and then rename it after the old ones are deleted
        final int segNumTemp = 99999;

        PageFileChannel fileHeaderA     = PageFileChannel.createOrOpen(Paths.get(getHeaderFilePathString(segNumA)));
        PageFileChannel fileHeaderB     = PageFileChannel.createOrOpen(Paths.get(getHeaderFilePathString(segNumB)));
        PageFileChannel fileHeaderNew   = PageFileChannel.createOrOpen(Paths.get(getHeaderFilePathString(segNumTemp)));

        PageFileChannel fileSegmentA      = PageFileChannel.createOrOpen(Paths.get(getSegmentFilePathString(segNumA)));
        PageFileChannel fileSegmentB      = PageFileChannel.createOrOpen(Paths.get(getSegmentFilePathString(segNumB)));
        PageFileChannel fileSegmentNew    = PageFileChannel.createOrOpen(Paths.get(getSegmentFilePathString(segNumTemp)));

        PageFileChannel filePositionA     = PageFileChannel.createOrOpen(Paths.get(getPositionFilePathString(segNumA)));
        PageFileChannel filePositionB     = PageFileChannel.createOrOpen(Paths.get(getPositionFilePathString(segNumB)));
        PageFileChannel filePositionNew   = PageFileChannel.createOrOpen(Paths.get(getPositionFilePathString(segNumTemp)));

        InvertedIndexIterator iteratorA = new InvertedIndexIterator(fileHeaderA, fileSegmentA, filePositionA);
        InvertedIndexIterator iteratorB = new InvertedIndexIterator(fileHeaderB, fileSegmentB, filePositionB);

        AutoFlushBuffer bufferHeaderFileNew = new AutoFlushBuffer(fileHeaderNew);
        AutoFlushBuffer bufferSegmentFileNew = new AutoFlushBuffer(fileSegmentNew);
        AutoFlushBuffer bufferPositionFileNew = new AutoFlushBuffer(filePositionNew);

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
                for (Integer integer : indexB.docPositions.keySet()){
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

        fileHeaderA.close();
        fileHeaderB.close();
        fileHeaderNew.close();

        fileSegmentA.close();
        fileSegmentB.close();
        fileSegmentNew.close();

        filePositionA.close();
        filePositionB.close();
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
            mergeSegments(i, i+1, i/2);
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
            PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(i)
                    ));
            PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(i)
                    ));

            AutoLoadBuffer headerFileBuffer = new AutoLoadBuffer(headerFileChannel);

            String word = "";
            int pageId = 0, offset = 0;

            while (headerFileBuffer.hasRemaining()) {
                int lenWords = headerFileBuffer.getInt();
                byte[] wordBytes = new byte[lenWords];
                for (int j = 0; j < lenWords; j++) {
                    wordBytes[j] = headerFileBuffer.getByte();
                }
                word = new String(wordBytes);
                pageId = headerFileBuffer.getInt();
                offset = headerFileBuffer.getInt();
                if (word.equals(keyword)){
                    break;
                }
            }

            if (!word.equals(keyword)){
                headerFileChannel.close();
                segmentFileChannel.close();
                continue;
            }

            Set<Integer> documentIds = getDocumentIds(segmentFileChannel, pageId, offset);
            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));

            for (Integer documentId : documentIds){
                results.add(documentStore.getDocument(documentId));
            }
            documentStore.close();
            headerFileChannel.close();
            segmentFileChannel.close();
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

        for (int i = 0; i < getNumSegments(); i++) {
            List<List<Integer>> header = new ArrayList<>();
            PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(i)
                    ));
            PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(i)
                    ));

            AutoLoadBuffer headerFileBuffer = new AutoLoadBuffer(headerFileChannel);

            while (headerFileBuffer.hasRemaining()) {
                int lenWords = headerFileBuffer.getInt();
                byte[] wordBytes = new byte[lenWords];
                for (int j = 0; j < lenWords; j++) {
                    wordBytes[j] = headerFileBuffer.getByte();
                }
                String word = new String(wordBytes);
                int pageId = headerFileBuffer.getInt();
                int offset = headerFileBuffer.getInt();
                if (words.contains(word)) {
                    List<Integer> paras = Arrays.asList(pageId, offset);
                    header.add(paras);
                }
            }

            if (header.size() != words.size()){
                headerFileChannel.close();
                segmentFileChannel.close();
                continue;
            }

            Set<Integer> intersection = getDocumentIds(segmentFileChannel, header.get(0).get(0), header.get(0).get(1));

            for (int j = 1; j < header.size(); j++){
                intersection.retainAll(getDocumentIds(segmentFileChannel, header.get(j).get(0), header.get(j).get(1)));
            }

            if (intersection.size() == 0){
                headerFileChannel.close();
                segmentFileChannel.close();
                continue;
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));
            for (Integer e : intersection){
                results.add(documentStore.getDocument(e));
            }

            documentStore.close();
            headerFileChannel.close();
            segmentFileChannel.close();
        }
        return results.iterator();
        // throw new UnsupportedOperationException();
    }

    public Set<Integer> getDocumentIds(PageFileChannel segmentFileChannel, int pageId, int offset){
        ByteBuffer buffer4Length = segmentFileChannel.readPage(pageId);
        buffer4Length.put(segmentFileChannel.readPage(pageId + 1));
        buffer4Length.position(offset);
        int numOccurrence = buffer4Length.getInt();
        Set<Integer> result = new HashSet<>(numOccurrence);
        int numPages = (int) Math.ceil((numOccurrence * 4 + offset + 0.0) / PAGE_SIZE);

        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE * numPages);
        for (int i = pageId; i < pageId + numPages; i++) {
            buffer.put(segmentFileChannel.readPage(i));
        }
        buffer.rewind();
        buffer.position(offset + 4);
        for (int j = 0; j < numOccurrence; j++) {
            result.add(buffer.getInt());
            buffer.getInt();
            buffer.getInt();
        }

        return result;
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

        List<Document> results = new ArrayList<>();
        List<String> words = new ArrayList<>();
        for (String keyword : keywords){
            words.add(analyzer.analyze(keyword).get(0));
        }

        for (int i = 0; i < getNumSegments(); i++) {
            List<List<Integer>> header = new ArrayList<>();
            PageFileChannel headerFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getHeaderFilePathString(i)
                    ));
            PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(
                    Paths.get(
                            getSegmentFilePathString(i)
                    ));

            AutoLoadBuffer headerFileBuffer = new AutoLoadBuffer(headerFileChannel);

            while (headerFileBuffer.hasRemaining()) {
                int lenWords = headerFileBuffer.getInt();
                byte[] wordBytes = new byte[lenWords];
                for (int j = 0; j < lenWords; j++) {
                    wordBytes[j] = headerFileBuffer.getByte();
                }
                String word = new String(wordBytes);
                int pageId = headerFileBuffer.getInt();
                int offset = headerFileBuffer.getInt();
                if (words.contains(word)) {
                    List<Integer> paras = Arrays.asList(pageId, offset);
                    header.add(paras);
                }
            }

            if (header.isEmpty()){
                headerFileChannel.close();
                segmentFileChannel.close();
                continue;
            }

            Set<Integer> union = getDocumentIds(segmentFileChannel, header.get(0).get(0), header.get(0).get(1));
            System.out.println(union);

            for (int j = 1; j < header.size(); j++){
                union.addAll(getDocumentIds(segmentFileChannel, header.get(j).get(0), header.get(j).get(1)));
            }

            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(i));
            for (Integer e : union){
                results.add(documentStore.getDocument(e));
            }

            documentStore.close();
            headerFileChannel.close();
            segmentFileChannel.close();
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
                    headerFileChannel, segmentFileChannel, positionFileChannel);

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
                        headerFileChannel, segmentFileChannel, positionFileChannel);
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

        DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segmentNum));
        Iterator<Map.Entry<Integer, Document>> it = documentStore.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Document> entry = it.next();
            documents.put(entry.getKey(), entry.getValue());
        }
        documentStore.close();

        Map<String, List<Integer>> invertedLists = new TreeMap<>();

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

        InvertedIndexIterator indexIterator = new InvertedIndexIterator(headerFileChannel, segmentFileChannel, positionFileChannel);
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

        headerFileChannel.close();
        segmentFileChannel.close();
        positionFileChannel.close();

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
