package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Iterators.*;
import edu.uci.ics.cs221.analysis.*;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

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
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    public static int PAGE_SIZE = 4096;

    private Analyzer analyzer;
    private String indexFolder;
    public TreeMap<String, List<Integer>> buffer = new TreeMap<>();
    public Integer record = 0;
    public Integer numStores = 0;
    private Integer numOfSeg = 0;
    public String docStorePath;

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
     * Adds a document to the inverted index.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     *
     * @param document
     */
    public void addDocument(Document document) {
        Preconditions.checkNotNull(document);
        docStorePath = "./docs" + numStores.toString() + ".db";
        Map<Integer, Document> documents = new TreeMap<>();
        documents.put(record, document);
        List<String> tokens = analyzer.analyze(document.getText());
        for (String token : tokens) {
            if (buffer.containsKey(token)) {
                List<Integer> orders = buffer.get(token);
                if (orders.get(orders.size() - 1).equals(record)) {
                    break;
                } else {
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
            Iterator<Map.Entry<Integer, Document>> itr = documents.entrySet().iterator();
            DocumentStore documentStore = MapdbDocStore.createWithBulkLoad(docStorePath, itr);
            documentStore.close();
            flush();
            numStores += 1;
            record = 0;
            buffer.clear();
        }
        //throw new UnsupportedOperationException();
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     */
    public void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        String path = "./index_path/header" + numOfSeg.toString() + ".txt";
        String path1 = "./index_path/segment" + numOfSeg.toString() + ".txt";
        int len = 0;
        int pageId, offset;
        numOfSeg += 1;
        Path filePath = Paths.get(path);
        PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
        for (String obj : buffer.keySet()) {
            len = len + obj.getBytes().length + 4 * 4;
        }
        pageId = 0;
        offset = 0;
        ByteBuffer buf = ByteBuffer.allocate(len);
        for (String words : buffer.keySet()) {
            int wordSize = buffer.get(words).size();
            buf.putInt(words.length());
            byte[] bytes = words.getBytes();
            buf.put(bytes);
            buf.putInt(pageId).putInt(offset).putInt(wordSize);
            if (wordSize < PAGE_SIZE - offset) {
                offset += wordSize;
            } else {
                pageId = pageId + 1 + (wordSize - (PAGE_SIZE - offset)) / PAGE_SIZE;
                offset = (wordSize - (PAGE_SIZE - offset)) % PAGE_SIZE;
            }
        }
        pageFileChannel.appendAllBytes(buf);
        buf.clear();
        pageFileChannel.close();

        Path filePath1 = Paths.get(path1);
        PageFileChannel pageFileChannel1 = PageFileChannel.createOrOpen(filePath1);
        int numOfInts = 0;
        for (List<Integer> appears : buffer.values()) {
            for (Integer i : appears) {
                numOfInts++;
            }
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(numOfInts * 4);
        for (List<Integer> appears : buffer.values()) {
            for (Integer i : appears) {
                byteBuffer.putInt(i);
            }
        }
        pageFileChannel1.appendAllBytes(byteBuffer);
        byteBuffer.clear();
        pageFileChannel1.close();
        if (numOfSeg == InvertedIndexManager.DEFAULT_MERGE_THRESHOLD) {
            mergeAllSegments();
        }
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
        PageFileChannel pagefile1 = PageFileChannel.createOrOpen(filePath1);
        ByteBuffer btf = pagefile1.readAllPages();
        btf.position(12);
        System.out.println(btf.getInt());
         */
        //throw new UnsupportedOperationException();
    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     */
    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        
        // throw new UnsupportedOperationException();
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
        
        // throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
       
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
        
        // throw new UnsupportedOperationException();
    }

    /**
     * Iterates through all the documents in all disk segments.
     */
    public Iterator<Document> documentIterator() {
        /*
        for (int i = 0; i <= numStores; i++){
            String docStorePath = "./docs" + i + ".db";
            DocumentStore documentStore = MapdbDocStore.createOrOpenReadOnly(docStorePath);
            documentStore.iterator().next().
        }
         */
        throw new UnsupportedOperationException();
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
        return numOfSeg;
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
        if (segmentNum >= numOfSeg)
            return null;
        String docStorePath1 = "./docs" + segmentNum + ".db";
        DocumentStore documentStore1 = MapdbDocStore.createOrOpenReadOnly(docStorePath1);
        Iterator<Integer> itr = documentStore1.keyIterator();
        Map<Integer, Document> documents = new HashMap<>();
        while (itr.hasNext()) {
            Integer inte = itr.next();
            Document doc = documentStore1.getDocument(inte);
            documents.put(inte, doc);
        }
        String path = "./index_path/header" + segmentNum + ".txt";
        Path filePath = Paths.get(path);
        PageFileChannel pageFileChannel = PageFileChannel.createOrOpen(filePath);
        String path1 = "./index_path/segment" + segmentNum + ".txt";
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
            if (wordLength == 0)
                break;
            byte[] dst = new byte[wordLength];
            btf.get(dst, 0, wordLength);
            String keyWord = new String(dst);
            int pageID = btf.getInt();
            int offset = btf.getInt();
            int length = btf.getInt();
            byte[] docs = new byte[length * 4];
            int pages;
            if (length <= PAGE_SIZE - offset) {
                pages = 1;
            } else {
                int extraPages = (length - (PAGE_SIZE - offset)) / PAGE_SIZE;
                int pos = (length - (PAGE_SIZE - offset)) % PAGE_SIZE;
                if (pos == 0) {
                    pages = extraPages + 1;
                } else {
                    pages = extraPages + 2;
                }
            }
            ByteBuffer buf = ByteBuffer.allocate(pages * PAGE_SIZE);
            for (int i = 0; i < pages; i++) {
                buf.put(pageFileChannel1.readPage(pageID + i));
            }
            buf.position(offset * 4);
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
        return new InvertedIndexSegmentForTest(invertedLists, documents);
        //throw new UnsupportedOperationException();
    }
}
