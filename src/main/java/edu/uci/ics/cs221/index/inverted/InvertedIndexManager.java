package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;
import org.checkerframework.checker.nullness.qual.Nullable;
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
     *          4 Bytes -> number of segment/occurrence of the word
     *
     *  Segment File:
     *      Each segment:
     *          First Document:
     *              4 Bytes -> the id of the first document that contains the word
     *              4 Bytes -> page id of the position of the word in the Position File
     *              4 Bytes -> offset of the position of the word in the Position File
     *              4 Bytes -> number of the positions of the word in the Position File
     *          Second Document:
     *              4 Bytes -> the id of the second document that contains the word
     *          ...
     *
     *   Position File:
     *       Each position:
     *          4 Bytes -> the first position of the word in the related document
     *          4 Bytes -> the second position of the word in the related document
     *          ...
     */

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
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
    public Map<Integer, Document> documents = new TreeMap<>();
    public TreeMap<String, List<Integer>> buffer = new TreeMap<>();
    private Table<String, Integer, List<Integer>> positions = HashBasedTable.create();
    public Integer numDocuments = 0;
    private Integer numSegments = 0;

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
        throw new UnsupportedOperationException();
    }


    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        Preconditions.checkNotNull(document);

        documents.put(numDocuments, document);
        List<String> tokens = analyzer.analyze(document.getText());
        for (int tokenPosition = 0; tokenPosition < tokens.size(); tokenPosition++) {
            String token = tokens.get(tokenPosition);

            if (buffer.containsKey(token)) {
                List<Integer> orders = buffer.get(token);
                if (orders.get(orders.size() - 1).equals(numDocuments) == false) {
                    orders.add(numDocuments);
                }
            } else {
                List<Integer> ids = new ArrayList<>();
                ids.add(numDocuments);
                buffer.put(token, ids);
            }

            List<Integer> positionInCurrentDocument = positions.get(token, numDocuments);
            if (positionInCurrentDocument == null) {
                positionInCurrentDocument = new ArrayList<>();
            }

            //System.out.println("Before " + positionInCurrentDocument);
            positionInCurrentDocument.add(tokenPosition);
            //System.out.println("After " + positionInCurrentDocument);
            positions.put(token, numDocuments, positionInCurrentDocument);

            //System.out.println(positions);
        }

        numDocuments += 1;
        if (numDocuments == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }

    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (buffer.isEmpty() && documents.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<Integer, Document>> itr = documents.entrySet().iterator();
        DocumentStore documentStore = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(numSegments), itr);
        documentStore.close();

        String headerFilePathString = getHeaderFilePathString(numSegments);
        int lenHeaderFile = 0;
        Path filePath = Paths.get(headerFilePathString);
        PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
        for (String obj : buffer.keySet()) {
            lenHeaderFile = lenHeaderFile + obj.getBytes().length + 4 * 4;
        }

        int pageId = 0, offset = 0;

        ByteBuffer buf = ByteBuffer.allocate(lenHeaderFile);

        for (String word : buffer.keySet()) {
            byte[] bytes = word.getBytes();
            //System.out.println(new String(bytes));
            buf.putInt(bytes.length);
            buf.put(bytes);
            int numOccurrence = buffer.get(word).size();
            //System.out.println(word);
            //System.out.println(word.length());
            //System.out.println(pageId);
            //System.out.println(offset);
            //System.out.println(numOccurrence);
            buf.putInt(pageId).putInt(offset).putInt(numOccurrence);
            // ToDo: fix the offset part in the getIndexSegement()
            int lenOccurInBytes = numOccurrence * 4;
            if (lenOccurInBytes < PAGE_SIZE - offset) {
                offset += lenOccurInBytes;
            } else {
                pageId = pageId + 1 + (lenOccurInBytes - (PAGE_SIZE - offset)) / PAGE_SIZE;
                offset = (lenOccurInBytes - (PAGE_SIZE - offset)) % PAGE_SIZE;
            }
        }
        pageFileChannel.appendAllBytes(buf);
        buf.clear();
        pageFileChannel.close();

        String segmentFilePathString = getSegmentFilePathString(numSegments);
        PageFileChannel segmentFileChannel = PageFileChannel.createOrOpen(Paths.get(segmentFilePathString));
        int totalNumAllOccurrence = 0;
        for (List<Integer> occurrences : buffer.values()) {
            totalNumAllOccurrence += occurrences.size();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalNumAllOccurrence * 4);
        for (List<Integer> appears : buffer.values()) {
            for (Integer i : appears) {
                byteBuffer.putInt(i);
            }
        }
        segmentFileChannel.appendAllBytes(byteBuffer);
        byteBuffer.clear();
        segmentFileChannel.close();

        String positionFilePathString = getPositionFilePathString(numSegments);
        PageFileChannel positionFileChannel = PageFileChannel.createOrOpen(Paths.get(positionFilePathString));
        /*
        // In progress
        int totalNumAllOccurrence = 0;
        for (List<Integer> occurrences : buffer.values()) {
            totalNumAllOccurrence += occurrences.size();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalNumAllOccurrence * 4);
        for (List<Integer> appears : buffer.values()) {
            for (Integer i : appears) {
                byteBuffer.putInt(i);
            }
        }
        segmentFileChannel.appendAllBytes(byteBuffer);
        byteBuffer.clear();
        segmentFileChannel.close();
         */

        if (numSegments == InvertedIndexManager.DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }

        numDocuments = 0;
        buffer.clear();
        documents.clear();

        numSegments += 1;
    }


    class HeaderFileRow {
        int lenWords;
        byte[] bytes;
        int pageId;
        int offset;
        int numOccurrence;
        List<Integer> occurrenceList;
        String keyword;

        @Override
        public String toString() {
            assert keyword.isEmpty() == false;
            return "keyword: " + keyword
                    + "\tpageId: " + pageId
                    + "\toffset: " + offset
                    + "\tnumOcc " + numOccurrence
                    + "\t occu " + occurrenceList.toString();
        }

        ByteBuffer getOccurrenceListByteBuffer() {
            return listInt2ByteBuffer(occurrenceList);
        }

        ByteBuffer getHeaderRowByteBuffer() {
            ByteBuffer result = ByteBuffer.allocate(keyword.getBytes().length + 4*4);

            // Note that keyword.getBytes().length can be larger than keyword.length()
            // because some characters are not ASCII but UTF-8 and take more than 1 char
            result.putInt(keyword.getBytes().length);
            result.put(keyword.getBytes());
            result.putInt(pageId);
            result.putInt(offset);
            numOccurrence = occurrenceList.size();
            result.putInt(numOccurrence);

            return result;
        }
    }


    class AutoLoadBuffer {
        ByteBuffer buffer;
        int pageId;
        PageFileChannel file;

        AutoLoadBuffer(PageFileChannel file) {
            pageId = 0;
            this.file = file;

            buffer = ByteBuffer.allocate(PAGE_SIZE);
            if (pageId < file.getNumPages()) {
                buffer = file.readPage(0);
            }
        }

        byte getByte() {
            byte[] result = new byte[1];
            buffer.get(result);

            if (buffer.hasRemaining() == false) {
                buffer.clear();
                pageId++;

                if (pageId < file.getNumPages()) {
                    //System.out.println("loading page " + pageId + " \\ " + file.getNumPages());
                    buffer = file.readPage(pageId);
                }
            }
            //System.out.println("got byte: " + result[0]);
            return result[0];
        }

        int getInt() {
            ByteBuffer tempBuffer = ByteBuffer.allocate(4);
            for (int i = 0; i < 4; i++) {
                tempBuffer.put(getByte());
            }

            tempBuffer.rewind();
            int result = tempBuffer.getInt();

            //System.out.println("get int: " + result);
            return result;
        }

        boolean hasRemaining() {
            //System.out.println("hasRemaining pageId: " + pageId + " numpages " + file.getNumPages());
            if (pageId < file.getNumPages() - 1) {
                return true;
            }

            if (pageId == file.getNumPages() - 1 && buffer.hasRemaining()) {
                if (buffer.position() < buffer.capacity() - 4) {
                    int nextWordLength = buffer.getInt();
                    buffer.position(buffer.position() - 4);
                    return nextWordLength > 0;
                }
            }

            return false;
        }
    }


    // Since the underlying file is based on pages, we need such an iterator to make life easier
    class HeaderFileRowIterator implements Iterator<HeaderFileRow> {
        PageFileChannel file, segmentFile;
        AutoLoadBuffer buffer;
        int pageNum, offset;

        HeaderFileRowIterator(PageFileChannel file, PageFileChannel segmentFile) {
            this.file = file;
            this.segmentFile = segmentFile;
            buffer = null;

            offset = 0;
            pageNum = 0;
            buffer = new AutoLoadBuffer(file);
        }

        @Override public boolean hasNext() {
            return buffer.hasRemaining();
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

        @Override public HeaderFileRow next() {
            if (hasNext() == false) {
                return null;
            }

            HeaderFileRow row = new HeaderFileRow();
            row.lenWords = buffer.getInt();
            row.bytes = new byte[row.lenWords];
            for (int i = 0; i < row.lenWords; i++) {
                row.bytes[i] = buffer.getByte();
            }
            row.pageId = buffer.getInt();
            row.offset = buffer.getInt();
            row.numOccurrence = buffer.getInt();

            row.keyword = new String(row.bytes);
            row.occurrenceList = getSegmentList(row.pageId, row.offset, row.numOccurrence);

            //System.out.println(row.toString());
            return row;
        }
    }


    class AutoFlushBuffer {
        ByteBuffer buffer;
        PageFileChannel file;

        AutoFlushBuffer(PageFileChannel file) {
            this.buffer = ByteBuffer.allocate(PAGE_SIZE);
            this.file = file;
        }

        AutoFlushBuffer put(ByteBuffer bytes) {
            //System.out.println("putting " + bytes.toString());
            bytes.rewind();

            while (bytes.hasRemaining()) {
                //System.out.println("cap " + bytes.capacity() + " pos " + bytes.position());

                byte[] tempByte = new byte[1];
                bytes.get(tempByte);
                buffer.put(tempByte);

                if (buffer.hasRemaining() == false) {
                    flush();
                }
            }

            //System.out.println("after putting " + buffer.toString());

            return this;
        }

        AutoFlushBuffer flush() {
            if (buffer.hasRemaining()) {
                byte byteZero = 0;

                while (buffer.hasRemaining()) {
                    buffer.put(byteZero);
                }
            }
            file.appendAllBytes(buffer);
            buffer.clear();
            return this;
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

        HeaderFileRowIterator headerFileRowIteratorA = new HeaderFileRowIterator(fileHeaderA, fileSegmentA);
        HeaderFileRowIterator headerFileRowIteratorB = new HeaderFileRowIterator(fileHeaderB, fileSegmentB);

        AutoFlushBuffer bufferHeaderFileNew = new AutoFlushBuffer(fileHeaderNew);
        AutoFlushBuffer bufferSegmentFileNew = new AutoFlushBuffer(fileSegmentNew);

        DocumentStore documentStoreA = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumA));
        DocumentStore documentStoreB = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segNumB));

        DocumentStore documentStoreNew = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(segNumTemp), new BulkLoadIterator(documentStoreA.iterator(), documentStoreB.iterator(),
                (int) documentStoreA.size()));

        HeaderFileRow rowA = null, rowB = null;
        if (headerFileRowIteratorA.hasNext()) {
            rowA = headerFileRowIteratorA.next();
        }
        if (headerFileRowIteratorB.hasNext()) {
            rowB = headerFileRowIteratorB.next();
        }
        int newPageId = 0, newOffset = 0;

        while (rowA != null || rowB != null) {

            HeaderFileRow rowNew = new HeaderFileRow();

            if (rowA != null && rowB != null && rowA.keyword.compareTo(rowB.keyword) == 0) {
                rowNew.keyword = rowA.keyword;
                rowNew.occurrenceList = new ArrayList<>(rowA.occurrenceList.size() + rowB.occurrenceList.size());
                rowNew.occurrenceList.addAll(rowA.occurrenceList);

                for (int occur : rowB.occurrenceList) {
                    // ToDo: maybe use long instead of int?
                    rowNew.occurrenceList.add(new Integer((int) (occur + documentStoreA.size())));
                }

                //System.out.println("\nrow A \t" + rowA.toString());
                //System.out.println("row B \t" + rowB.toString());

                rowA = headerFileRowIteratorA.next();
                rowB = headerFileRowIteratorB.next();

            } else if ( rowA != null && (rowB == null || rowA.keyword.compareTo(rowB.keyword) < 0) ) {
                rowNew = rowA;

                //System.out.println("\nrow A \t" + rowA.toString());

                rowA = headerFileRowIteratorA.next();
            } else if (rowB != null && (rowA == null || rowA.keyword.compareTo(rowB.keyword) > 0)) {
                rowNew = rowB;
                for (int i = 0; i < rowNew.occurrenceList.size(); i++) {
                    rowNew.occurrenceList.set(i, (int) (rowNew.occurrenceList.get(i) + documentStoreA.size()));
                }

                //System.out.println("\nrow B \t" + rowB.toString());

                rowB = headerFileRowIteratorB.next();
            }

            rowNew.pageId = newPageId;
            rowNew.offset = newOffset;
            bufferHeaderFileNew.put(rowNew.getHeaderRowByteBuffer());
            bufferSegmentFileNew.put(rowNew.getOccurrenceListByteBuffer());

            newPageId = newPageId + (newOffset + rowNew.numOccurrence * 4) / PAGE_SIZE;
            newOffset = (newOffset + rowNew.numOccurrence * 4) % PAGE_SIZE;

            //System.out.println("new row\t" + rowNew.toString());
        }

        bufferHeaderFileNew.flush();
        bufferSegmentFileNew.flush();

        fileHeaderA.close();
        fileHeaderB.close();
        fileHeaderNew.close();

        fileSegmentA.close();
        fileSegmentB.close();
        fileSegmentNew.close();

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
            String headerFilePathString = getHeaderFilePathString(i);
            PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(Paths.get(headerFilePathString));
            ByteBuffer btf = pageFileChannel.readAllPages();
            btf.flip();
            int pageID=0, offset=0, length=0;
            String key = "";
            while (btf.hasRemaining()) {
                int wordLength = btf.getInt();
                if (wordLength == 0)
                    break;
                byte[] dst = new byte[wordLength];
                btf.get(dst, 0, wordLength);
                pageID = btf.getInt();
                offset = btf.getInt();
                length = btf.getInt();
                key = new String(dst);
                if (keyword.equals(key)) {
                    break;
                }
            }
            //System.out.println(key);
            //System.out.println(pageID);
            //System.out.println(offset);
            //System.out.println(length);
            if (!key.equals(keyword)){
                return Collections.emptyIterator();
            }

            String segmentFilePathString = getSegmentFilePathString(i);
            //System.out.println(segmentFilePathString);
            PageFileChannel pageFileChannel1 = PageFileChannel.createOrOpen(Paths.get(segmentFilePathString));
            byte[] docs = new byte[length * 4];
            int pages;
            if (length*4 <= PAGE_SIZE - offset) {
                pages = 1;
            } else {
                int extraPages = (length*4 - (PAGE_SIZE - offset)) / PAGE_SIZE;
                int pos = (length*4 - (PAGE_SIZE - offset)) % PAGE_SIZE;
                if (pos == 0) {
                    pages = extraPages + 1;
                } else {
                    pages = extraPages + 2;
                }
            }
            ByteBuffer buf = ByteBuffer.allocate(pages * PAGE_SIZE);
            for (int j = 0; j < pages; j++) {
                buf.put(pageFileChannel1.readPage(pageID + j));
            }
            buf.position(offset);
            buf.get(docs, 0, length * 4);
            ByteBuffer dor = ByteBuffer.allocate(length * 4);
            dor.put(docs);
            dor.flip();
            List<Integer> docIDs = new ArrayList<>();
            while (dor.hasRemaining()) {
                int docID = dor.getInt();
                //System.out.println(docID);
                docIDs.add(docID);
            }
            String docStorePath1 = getDocumentStorePathString(i);
            DocumentStore documentStore1 = MapdbDocStore.createOrOpenReadOnly(docStorePath1);
            for (Integer e : docIDs){
                results.add(documentStore1.getDocument(e));
            }
            documentStore1.close();
            pageFileChannel.close();
            pageFileChannel1.close();
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
        for (int i = 0; i < getNumSegments(); i++) {
            Map<String, List<Integer>> header = new TreeMap<>();
            String path = indexFolder + "/header" + i + ".txt";
            Path filePath = Paths.get(path);
            PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
            ByteBuffer btf = pageFileChannel.readAllPages();
            pageFileChannel.close();
            btf.flip();
            while (btf.hasRemaining()) {
                int wordLength = btf.getInt();
                if (wordLength == 0)
                    break;
                byte[] dst = new byte[wordLength];
                btf.get(dst, 0, wordLength);
                String key = new String(dst);
                int pageID = btf.getInt();
                int offset = btf.getInt();
                int length = btf.getInt();
                List<Integer> paras = Arrays.asList(pageID, offset, length);
                header.put(key, paras);
                }
            btf.clear();
            List<String> keys = new ArrayList<>(header.keySet());
            List<Set<Integer>> listOfWords = new ArrayList<>();
            for (String keyword : keywords) {
                keyword = analyzer.analyze(keyword).get(0);
                if (keys.contains(keyword)) {
                    List<Integer> nums = header.get(keyword);
                    Set<Integer> docIDs = getIDs(i, nums.get(0), nums.get(1), nums.get(2));
                    listOfWords.add(docIDs);
                } else {
                    return Collections.emptyIterator();
                }
            }
            if (listOfWords.size() == 0){
                return Collections.emptyIterator();
            }
            Set<Integer> word1 = listOfWords.get(0);
            for (int j = 1; j < listOfWords.size(); j++){
                word1.retainAll(listOfWords.get(j));
            }
            String docStorePath1 = getDocumentStorePathString(i);
            DocumentStore documentStore1 = MapdbDocStore.createOrOpenReadOnly(docStorePath1);
            for (Integer e : word1){
                results.add(documentStore1.getDocument(e));
            }
            documentStore1.close();
        }
        return results.iterator();
        // throw new UnsupportedOperationException();
    }

    public Set<Integer> getIDs(int i, int pageID, int offset, int length){
        String path1 = indexFolder + "/segment" + i + ".txt";
        Path filePath1 = Paths.get(path1);
        PageFileChannel pageFileChannel1 = PageFileChannel.createOrOpen(filePath1);
        byte[] docs = new byte[length * 4];
        int pages;
        if (length*4 <= PAGE_SIZE - offset) {
            pages = 1;
        } else {
            int extraPages = (length*4 - (PAGE_SIZE - offset)) / PAGE_SIZE;
            int pos = (length*4 - (PAGE_SIZE - offset)) % PAGE_SIZE;
            if (pos == 0) {
                pages = extraPages + 1;
            } else {
                pages = extraPages + 2;
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(pages * PAGE_SIZE);
        for (int j = 0; j < pages; j++) {
            buf.put(pageFileChannel1.readPage(pageID + j));
        }
        buf.position(offset);
        buf.get(docs, 0, length * 4);
        ByteBuffer dor = ByteBuffer.allocate(length * 4);
        dor.put(docs);
        dor.flip();
        Set<Integer> docIDs = new HashSet<>();
        while (dor.hasRemaining()) {
            int docID = dor.getInt();
            docIDs.add(docID);
        }
        pageFileChannel1.close();
        return docIDs;
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
        for (int i = 0; i < getNumSegments(); i++) {
            Map<String, List<Integer>> header = new HashMap<>();
            String path = indexFolder + "/header" + i + ".txt";
            Path filePath = Paths.get(path);
            PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
            ByteBuffer btf = pageFileChannel.readAllPages();
            pageFileChannel.close();
            btf.flip();
            while (btf.hasRemaining()) {
                int wordLength = btf.getInt();
                if (wordLength == 0)
                    break;
                byte[] dst = new byte[wordLength];
                btf.get(dst, 0, wordLength);
                String key = new String(dst);
                int pageID = btf.getInt();
                int offset = btf.getInt();
                int length = btf.getInt();
                List<Integer> paras = Arrays.asList(pageID, offset, length);
                header.put(key, paras);
            }
            //System.out.println(header);
            btf.clear();
            Set<String> keys = new HashSet<>(header.keySet());
            List<Set<Integer>> listOfWords = new ArrayList<>();
            for (String keyword : keywords) {
                keyword = analyzer.analyze(keyword).get(0);
                if (keys.contains(keyword)) {
                    List<Integer> nums = header.get(keyword);
                    Set<Integer> docIDs = getIDs(i, nums.get(0), nums.get(1), nums.get(2));
                    listOfWords.add(docIDs);
                }
            }
            Set<Integer> union = new HashSet<>();
            for (Set<Integer> ir : listOfWords){
                union.addAll(ir);
                }
            //System.out.println(union);
            String docStorePath1 = getDocumentStorePathString(i);
            DocumentStore documentStore1 = MapdbDocStore.createOrOpenReadOnly(docStorePath1);
            for (Integer e : union){
                results.add(documentStore1.getDocument(e));
            }
            documentStore1.close();
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

        throw new UnsupportedOperationException();
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

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {

        if (segmentNum >= numSegments)
            return null;

        DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(getDocumentStorePathString(segmentNum));
        Iterator<Integer> itr = documentStore.keyIterator();
        Map<Integer, Document> documents = new HashMap<>();

        while (itr.hasNext()) {
            Integer inte = itr.next();
            Document doc = documentStore.getDocument(inte);
            documents.put(inte, doc);
        }
        documentStore.close();
        String path = getHeaderFilePathString(segmentNum);
        Path filePath = Paths.get(path);
        PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
        String path1 = getSegmentFilePathString(segmentNum);
        Path filePath1 = Paths.get(path1);
        PageFileChannel pageFileChannel1 = PageFileChannel.createOrOpen(filePath1);
        ByteBuffer btf = pageFileChannel.readAllPages();
        /*
        btf.position(4);
        byte [] string = new byte[3];
        btf.get(string, 0, 3);
        String s = new String(string);
        System.out.println(s);
        btf.flip();
         */
        btf.flip();
        Map<String, List<Integer>> invertedLists = new TreeMap<>();
        while (btf.hasRemaining()) {

            int wordLength = btf.getInt();
            //System.out.println("wordLen " + wordLength);
            if (wordLength == 0)
                break;

            byte[] dst = new byte[wordLength];
            //System.out.println("pos " + btf.position() + " cap " + btf.capacity());
            btf.get(dst, 0, wordLength);
            //System.out.println(btf.position());
            String keyWord = new String(dst);

            int pageID = btf.getInt();
            int offset = btf.getInt();
            int length = btf.getInt();
            //System.out.println("keyword " + keyWord + " pageId " + Integer.toHexString(pageID) + " offset " + Integer.toHexString(offset) + " len " + Integer.toHexString(length));

            byte[] docs = new byte[length * 4];
            int pages;
            //System.out.println(pageID);
            //System.out.println(offset);
            //System.out.println(length);
            if (length*4 <= PAGE_SIZE - offset) {
                pages = 1;
            } else {
                int extraPages = (length*4 - (PAGE_SIZE - offset)) / PAGE_SIZE;
                int pos = (length*4 - (PAGE_SIZE - offset)) % PAGE_SIZE;
                if (pos == 0) {
                    pages = extraPages + 1;
                } else {
                    pages = extraPages + 2;
                }
            }
            //System.out.println(pages);
            ByteBuffer buf = ByteBuffer.allocate(pages * PAGE_SIZE);
            for (int i = 0; i < pages; i++) {
                buf.put(pageFileChannel1.readPage(pageID + i));
            }
            buf.position(offset);
            buf.get(docs, 0, length * 4);
            ByteBuffer dor = ByteBuffer.allocate(length * 4);
            dor.put(docs);
            dor.flip();
            List<Integer> docIDs = new ArrayList<>();
            while (dor.hasRemaining()) {
                int docID = dor.getInt();
                docIDs.add(docID);
            }
            invertedLists.put(keyWord, docIDs);
        }
        //System.out.println(invertedLists);
        pageFileChannel.close();
        pageFileChannel1.close();

        return new InvertedIndexSegmentForTest(invertedLists, documents);
        //throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

}
