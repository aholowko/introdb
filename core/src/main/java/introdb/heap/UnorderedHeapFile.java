package introdb.heap;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;

class UnorderedHeapFile implements Store {

    private final FileChannel file;
    private final byte[] zeros;
    private final ByteBuffer pageCache;
    private final int maxNrPages;
    private final int pageSize;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) throws IOException {
        this.file = FileChannel.open(path,
                Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
        this.maxNrPages = maxNrPages;
        this.pageSize = pageSize;
        this.zeros = new byte[pageSize];
        this.pageCache = ByteBuffer.allocate(pageSize);
    }

    @Override
    public synchronized void put(Entry entry) throws IOException, ClassNotFoundException {
        log("Putting the entry: " + entry);
        final Record record = Record.of(entry);
        final int recordSize = record.size();
        if (recordSize > pageSize) {
            throw new IllegalArgumentException("Unsupported entry size. It's too big.");
        }
        final ByteBuffer page = preparePage();
        int currentPage = 0;
        while (currentPage < maxNrPages) {
            log("Reading the page " + currentPage + " ...");
            final int bytesRead = readPage(page, currentPage);
            if (bytesRead == -1) {
                //eof
                log("EOF. Write the record here");
                writeRecordToPage(record, page, currentPage);
                return;
            }
            page.rewind();
            //read records from the page
            do {
                page.mark(); //mark the beginning of the record
                final Record readRecord = Record.readFrom(page);
                log("Read record from the page " + currentPage + ": " + readRecord);
                if (readRecord == null) {
                    //no more records in page
                    page.reset(); //reset to the beginning of the record
                    writeRecordToPage(record, page, currentPage);
                    return;
                }
                if (isRecordWithKey(readRecord, record.key())) {
                    //record found
                    log("Marking record as removed: " + readRecord);
                    readRecord.markAsRemoved();
                    page.reset(); //reset to the beginning of the record
                    writeRecordToPage(readRecord, page, currentPage);
                }
            } while (page.remaining() >= recordSize);
            currentPage++;
        }
        throw new EOFException();
    }

    @Override
    public synchronized Object get(Serializable key) throws IOException, ClassNotFoundException {
        log("Looking for a key: " + key);
        final byte[] keySerialized = Record.serialize(key);
        final ByteBuffer page = preparePage();
        int currentPage = 0;
        while (currentPage < maxNrPages) {
            log("Reading page " + currentPage + "...");
            final int bytesRead = readPage(page, currentPage);
            if (bytesRead == -1) {
                //eof
                return null;
            }
            page.rewind();
            //read records from page
            do {
                log("Reading record from page " + currentPage + "...");
                final Record readRecord = Record.readFrom(page);
                if (readRecord == null) {
                    //no more records in the current page
                    break;
                }
                if (isRecordWithKey(readRecord, keySerialized)) {
                    return readRecord.valueDeserialized();
                }
            } while (page.hasRemaining());
            currentPage++;
        }
        throw new EOFException();
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        final byte[] keySerialized = Record.serialize(key);
        final ByteBuffer page = preparePage();
        int currentPage = 0;
        while (currentPage < maxNrPages) {
            log("Reading the page " + currentPage + " ...");
            //readPage
            final int bytesRead = readPage(page, currentPage);
            if (bytesRead == -1) {
                //eof
                log("End of file");
                return null;
            }
            page.rewind();
            //read records from page
            do {
                page.mark(); //mark the beginning of the record
                final Record readRecord = Record.readFrom(page);
                log("Read record from the page " + currentPage + ": " + readRecord);
                if (readRecord == null) {
                    log("End of page");
                    break;
                }
                if (isRecordWithKey(readRecord, keySerialized)) {
                    log("Marking record as removed: " + readRecord);
                    page.reset(); //reset to the beginning of the record
                    readRecord.markAsRemoved();
                    writeRecordToPage(readRecord, page, currentPage);
                    return readRecord.valueDeserialized();
                }
            } while (page.hasRemaining());
            currentPage++;
        }
        throw new EOFException();
    }

    private ByteBuffer preparePage() {
        clearPage(pageCache);
        return pageCache;
    }

    private boolean isRecordWithKey(Record readRecord, byte[] key) {
        return readRecord.isAlive() && Arrays.equals(readRecord.key(), key);
    }

    private void writeRecordToPage(Record record, ByteBuffer page, int currentPage) throws IOException {
        record.writeTo(page);
        writePage(page, currentPage);
    }

    private void writePage(ByteBuffer page, int pageNr) throws IOException {
        page.rewind();
        file.write(page, pageNr * pageSize);
    }

    private int readPage(ByteBuffer page, int pageNr) throws IOException {
        clearPage(page);
        return file.read(page, pageNr * pageSize);
    }

    private void clearPage(ByteBuffer page) {
        page.clear();
        page.put(zeros);
        page.rewind();
    }

    private void log(String msg) {
//        System.out.println(msg);
    }
}

