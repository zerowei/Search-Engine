package edu.uci.ics.cs221.storage;

import com.google.common.collect.Iterators;
import org.mapdb.*;

import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.immutableEntry;

/**
 * MapdbDocStore implements the Document Store using a Library `MapDB` http://www.mapdb.org/
 *
 */
public class MapdbDocStore implements DocumentStore {

    private static final String mapName = "docStore";

    /**
     * Opens a document store at the given file location if it already exists.
     * Otherwise, creates a new document store at the give location and opens it.
     *
     * @param docStoreFile file location of the document store
     * @return handle to an opened Document Store
     */
    public static DocumentStore createOrOpen(String docStoreFile) {
        return new MapdbDocStore(docStoreFile);
    }

    private DB db;
    private BTreeMap<Integer, String> map;

    private MapdbDocStore(String docStoreFile) {
        this.db = DBMaker.fileDB(docStoreFile).make();
        this.map = this.db.treeMap(mapName)
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.STRING)
                .counterEnable()
                .createOrOpen();
    }

    @Override
    public void close() {
        if (this.map != null) {
            this.map.close();
        }
        if (this.db != null) {
            this.db.close();
        }
    }

    @Override
    public void addDocument(int docID, Document document) {
        checkNotNull(document);
        this.map.put(docID, document.getText());
    }

    @Override
    public Document getDocument(int docID) {
        String docText = this.map.get(docID);
        if (docText == null) {
            return null;
        }
        return new Document(docText);
    }

    @Override
    public Iterator<Map.Entry<Integer, Document>> iterator() {
        return Iterators.transform(this.map.getEntries().iterator(),
                entry -> immutableEntry(entry.getKey(), new Document(entry.getValue())));
    }

    @Override
    public Iterator<Integer> keyIterator() {
        return this.map.keyIterator();
    }

    @Override
    public long size() {
        return this.map.sizeLong();
    }

}
