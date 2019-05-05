package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
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
    public static int DEFAULT_MERGE_THRESHOLD = 11;

    public static int PAGE_SIZE = 4096;

    private Analyzer analyzer;
    private String indexFolder;
    public TreeMap<String, List<Integer>> buffer = new TreeMap<>();
    public Map<Integer, Document> documents = new TreeMap<>();
    public Integer record = 0;
    private Integer numSegments = 0;

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

    private String getDocumentStorePathString(int storeNum) {
        return indexFolder + "/docs" + storeNum + ".db";
    }

    /**
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        Preconditions.checkNotNull(document);
        documents.put(record, document);
        List<String> tokens = analyzer.analyze(document.getText());
        for (String token : tokens) {
            if (buffer.containsKey(token)) {
                List<Integer> orders = buffer.get(token);
                if (!orders.get(orders.size() - 1).equals(record)) {
                    orders.add(record);
                }
            } else {
                List<Integer> ids = new ArrayList<>();
                ids.add(record);
                buffer.put(token, ids);
            }
        }
        record += 1;
        if (record == DEFAULT_FLUSH_THRESHOLD) {
            flush();
        }
        //throw new UnsupportedOperationException();
    }

    // Test cases fail if return Paths.get() directly here
    private String getHeaderFilePathString(int segmentNum) {
        return indexFolder + "/header" + segmentNum + ".txt";
    }

    private String getSegmentFilePathString(int segmentNum) {
        return indexFolder + "/segment" + segmentNum + ".txt";
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (buffer.isEmpty() && documents.isEmpty()) {
            return;
        }
        System.out.println(buffer);
        Iterator<Map.Entry<Integer, Document>> itr = documents.entrySet().iterator();
        DocumentStore documentStore = MapdbDocStore.createWithBulkLoad(getDocumentStorePathString(numSegments), itr);
        documentStore.close();
        String headerFilePathString = getHeaderFilePathString(numSegments);
        String segmentFilePathString = getSegmentFilePathString(numSegments);
        int len = 0;
        numSegments += 1;
        Path filePath = Paths.get(headerFilePathString);
        PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
        for (String obj : buffer.keySet()) {
            len = len + obj.getBytes().length + 4 * 4;
        }

        int pageId = 0, offset = 0;

        ByteBuffer buf = ByteBuffer.allocate(len);

        for (String word : buffer.keySet()) {
            byte[] bytes = word.getBytes();
            System.out.println(new String(bytes));
            buf.putInt(bytes.length);
            buf.put(bytes);
            int numOccurrence = buffer.get(word).size();
            System.out.println(word);
            System.out.println(word.length());
            System.out.println(pageId);
            System.out.println(offset);
            System.out.println(numOccurrence);
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

        if (numSegments == InvertedIndexManager.DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }

        record = 0;
        buffer.clear();
        documents.clear();
        /*
        PageFileChannel pagefile1 = PageFileChannel.createOrOpen(filePath);
        ByteBuffer btf = pagefile1.readPage(0);
        btf.position(4);
        byte [] string = new byte[3];
        btf.get(string, 0, 3);
        String s = new String(string);
        System.out.println(s);
         */
        /*
        PageFileChannel pagefile1 = PageFileChannel.createOrOpen(segmentFilePath);
        ByteBuffer btf = pagefile1.readAllPages();
        btf.position(12);
        System.out.println(btf.getInt());
         */
        //throw new UnsupportedOperationException();
    }


    private HeaderFileRow decodeHeaderFileRow(byte[] bytes) {
        throw new NotImplementedException();
        //return new HeaderFileRow();
    }

    private byte[] eecodeHeaderFileRow(HeaderFileRow row) {
        throw new NotImplementedException();
        //return new HeaderFileRow();
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

            result.putInt(keyword.length());
            result.put(keyword.getBytes());
            result.putInt(pageId);
            result.putInt(offset);
            numOccurrence = occurrenceList.size();
            result.putInt(numOccurrence);

            return result;
        }
    }


    // Since the underlying file is based on pages, we need such an iterator to make life easier
    class HeaderFileRowIterator implements Iterator<HeaderFileRow> {
        PageFileChannel file, segmentFile;
        ByteBuffer buffer;
        int pageNum, offset;

        HeaderFileRowIterator(PageFileChannel file, PageFileChannel segmentFile) {
            this.file = file;
            this.segmentFile = segmentFile;
            buffer = null;

            offset = 0;
            pageNum = 0;
            if (pageNum < file.getNumPages()) {
                buffer = file.readPage(pageNum);
            }
        }

        @Override public boolean hasNext() {
            if (pageNum >= file.getNumPages()) {
                return false;
            }

            int nextLenWord = buffer.getInt();
            buffer.position(buffer.position()-4);

            if (nextLenWord > 0) {
                return true;
            }

            return false;
        }

        // We can add a wrapper to buffer such as AutoFlushBuffer to avoid calling loadNextPageIfNecessary() multiple times
        private void loadNextPageIfNecessary() {
            if (buffer.position() >= buffer.capacity()) {
                buffer.clear();
                pageNum++;

                if (pageNum < file.getNumPages()) {
                    buffer = file.readPage(pageNum);
                }
            }

        }

        // ToDo: cache the current page to avoid multiple read on the same page
        private List<Integer> getSegmentList(int pageId, int offset, int numOccurrence) {
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
            loadNextPageIfNecessary();
            row.bytes = new byte[row.lenWords];
            loadNextPageIfNecessary();
            for (int i = 0; i < row.lenWords; i++) {
                buffer.get(row.bytes, i, 1);
                loadNextPageIfNecessary();
            }
            row.pageId = buffer.getInt();
            loadNextPageIfNecessary();
            row.offset = buffer.getInt();
            loadNextPageIfNecessary();
            row.numOccurrence = buffer.getInt();
            loadNextPageIfNecessary();

            row.keyword = new String(row.bytes);
            row.occurrenceList = getSegmentList(row.pageId, row.offset, row.numOccurrence);

            //System.out.println(row.toString());
            return row;
        }
    }

    private void mergeDocumentStores(int docStoreNumA, int docStoreNumB, int docStoreNumNew) {

    }

    class AutoFlushBuffer {
        ByteBuffer buffer;
        PageFileChannel file;

        AutoFlushBuffer(PageFileChannel file) {
            this.buffer = ByteBuffer.allocate(PAGE_SIZE * 10);
            this.file = file;
        }

        AutoFlushBuffer put(ByteBuffer bytes) {
            System.out.println("putting " + bytes.toString());
            bytes.rewind();
            buffer.put(bytes);
            System.out.println("after putting " + buffer.toString());

            if (buffer.position() > PAGE_SIZE) {
                flush();
            }

            return this;
        }

        AutoFlushBuffer flush() {
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
                System.out.println("Deleted " + name + " successfully");
            } else {
                System.out.println("Failed to delete " + name);
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
                System.out.println("Rename " + entry.getKey() + " to " + entry.getValue() + " successfully");
            } else {
                System.out.println("Fail to rename " + entry.getKey() + " to " + entry.getValue());
            }
        }

        // Do a full copy to avoid checksum failure of MapDB
        DocumentStore oldStore = MapdbDocStore.createOrOpen(getDocumentStorePathString(oldSegNum));
        DocumentStore newStore = MapdbDocStore.createOrOpen(getDocumentStorePathString(newSegNum));
        for (Iterator<Map.Entry<Integer, Document>> iter = oldStore.iterator(); iter.hasNext();) {
            Map.Entry<Integer, Document> entry = iter.next();
            newStore.addDocument(entry.getKey(), entry.getValue());
        }
        oldStore.close();
        newStore.close();

        File file = new File(getDocumentStorePathString(oldSegNum));
        if ( file.delete() ) {
            System.out.println("Deleted " + getDocumentStorePathString(oldSegNum) + " successfully");
        } else {
            System.out.println("Failed to delete " + getDocumentStorePathString(oldSegNum));
        }

        return;
    }

    private void mergeSegments(int segNumA, int segNumB, int segNumNew) {
        System.out.println("------- merging " + segNumA + " and " + segNumB);

        int offsetHeaderFileA = 0, offsetHeaderFileB = 0;
        int offsetSegmentFileA = 0, offsetSegmentFileB = 0;
        int numDocumentA = 0;

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
        DocumentStore documentStoreNew = MapdbDocStore.createOrOpen(getDocumentStorePathString(segNumTemp));

        for (Iterator<Map.Entry<Integer, Document>> it = documentStoreA.iterator(); it.hasNext();) {
            Map.Entry<Integer, Document> docEntry = it.next();
            documentStoreNew.addDocument(docEntry.getKey(), docEntry.getValue());
        }
        for (Iterator<Map.Entry<Integer, Document>> it = documentStoreB.iterator(); it.hasNext();) {
            Map.Entry<Integer, Document> docEntry = it.next();
            documentStoreNew.addDocument((int) (docEntry.getKey() + documentStoreA.size()), docEntry.getValue());
        }

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

                System.out.println("\nrow A \t" + rowA.toString());
                System.out.println("row B \t" + rowB.toString());

                rowA = headerFileRowIteratorA.next();
                rowB = headerFileRowIteratorB.next();

            } else if ( rowA != null && (rowB == null || rowA.keyword.compareTo(rowB.keyword) < 0) ) {
                rowNew = rowA;

                System.out.println("\nrow A \t" + rowA.toString());

                rowA = headerFileRowIteratorA.next();
            } else if (rowB != null && (rowA == null || rowA.keyword.compareTo(rowB.keyword) > 0)) {
                rowNew = rowB;
                for (int i = 0; i < rowNew.occurrenceList.size(); i++) {
                    rowNew.occurrenceList.set(i, (int) (rowNew.occurrenceList.get(i) + documentStoreA.size()));
                }

                System.out.println("\nrow B \t" + rowB.toString());

                rowB = headerFileRowIteratorB.next();
            }

            rowNew.pageId = newPageId;
            rowNew.offset = newOffset;
            bufferHeaderFileNew.put(rowNew.getHeaderRowByteBuffer());
            bufferSegmentFileNew.put(rowNew.getOccurrenceListByteBuffer());

            newPageId = newPageId + (newOffset + rowNew.numOccurrence * 4) / PAGE_SIZE;
            newOffset = (newOffset + rowNew.numOccurrence * 4) % PAGE_SIZE;

            System.out.println("new row\t" + rowNew.toString());
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
            System.out.println(key);
            System.out.println(pageID);
            System.out.println(offset);
            System.out.println(length);
            if (!key.equals(keyword)){
                return Collections.emptyIterator();
            }

            String segmentFilePathString = getSegmentFilePathString(i);
            System.out.println(segmentFilePathString);
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
                System.out.println(docID);
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
            int mid = -1, low = 0, high = keys.size() - 1;
            String lastWord = ""; Boolean flag = false;
            for (String keyword : keywords) {
                keyword = analyzer.analyze(keyword).get(0);
                if (keyword.compareTo(lastWord) > 0){
                    low = mid + 1;
                }
                else {
                    high = mid - 1;
                }
                while (low <= high) {
                    mid = (low + high) / 2;
                    String key = keys.get(mid);
                    if (keyword.equals(key)) {
                        flag = true;
                        lastWord = key;
                        low = 0; high = keys.size() - 1;
                        List<Integer> nums = header.get(key);
                        Set<Integer> docIDs = getIDs(i, nums.get(0), nums.get(1), nums.get(2));
                        listOfWords.add(docIDs);
                        break;
                    } else if (keyword.compareTo(key) > 0) {
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                }
                if (!flag){
                    break;
                }
                else {
                    flag = false;
                }
            }
            if (keywords.size() > listOfWords.size()){
                break;
            }
            Set<Integer> mix = new HashSet<>();
            Set<Integer> word1 = listOfWords.get(0);
            for (int j = 1; j < listOfWords.size(); j++){
                for (Integer ir : listOfWords.get(j)){
                    if (word1.contains(ir)){
                        mix.add(ir);
                    }
                }
                word1.clear();
                word1.addAll(mix);
                mix.clear();
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
            System.out.println(header);
            btf.clear();
            List<String> keys = new ArrayList<>(header.keySet());
            List<Set<Integer>> listOfWords = new ArrayList<>();
            int mid = -1, low = 0, high = keys.size() - 1;
            String lastWord = "";
            Boolean flag = false;
            for (String keyword : keywords) {
                keyword = analyzer.analyze(keyword).get(0);
                if (keyword.compareTo(lastWord) > 0){
                    low = mid + 1;
                }
                else {
                    high = mid - 1;
                }
                while (low <= high) {
                    mid = (low + high) / 2;
                    String key = keys.get(mid);
                    if (keyword.equals(key)) {
                        lastWord = key;
                        flag = true;
                        List<Integer> nums = header.get(key);
                        Set<Integer> docIDs = getIDs(i, nums.get(0), nums.get(1), nums.get(2));
                        listOfWords.add(docIDs);
                        System.out.println(listOfWords);
                        break;
                    } else if (keyword.compareTo(key) > 0) {
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                }
                if (flag) {
                    low = 0;
                    high = keys.size() - 1;
                }
                else {
                    low = 0;
                    high = keys.size() - 1;
                    mid = -1;
                    lastWord = "";
                }
            }
            Set<Integer> union = new HashSet<>();
            for (Set<Integer> ir : listOfWords){
                union.addAll(ir);
                }
            System.out.println(union);
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
            System.out.println(wordLength);
            if (wordLength == 0)
                break;
            byte[] dst = new byte[wordLength];
            System.out.println(btf.position());
            btf.get(dst, 0, wordLength);
            System.out.println(btf.position());
            String keyWord = new String(dst);
            int pageID = btf.getInt();
            int offset = btf.getInt();
            int length = btf.getInt();
            System.out.println(keyWord);
            byte[] docs = new byte[length * 4];
            int pages;
            System.out.println(pageID);
            System.out.println(offset);
            System.out.println(length);
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
            System.out.println(pages);
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
        System.out.println(invertedLists);
        pageFileChannel.close();
        pageFileChannel1.close();

        return new InvertedIndexSegmentForTest(invertedLists, documents);
        //throw new UnsupportedOperationException();
    }
}
