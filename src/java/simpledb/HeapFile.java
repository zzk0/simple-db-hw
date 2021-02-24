package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private RandomAccessFile tableDataFile;
    private File originFile;
    private TupleDesc tupleDesc;
    private final int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        try {
            tableDataFile = new RandomAccessFile(f, "rw");
        }
        catch (FileNotFoundException e) {
            // Assume File f is valid
            e.printStackTrace();
        }
        originFile = f;
        tupleDesc = td;
        id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return originFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) {
        // some code goes here
        long startPos = BufferPool.getPageSize() * pid.getPageNumber();
        try {
            tableDataFile.seek(startPos);
            // In disk, the Pages also contain header and tuples
            byte[] data = new byte[BufferPool.getPageSize()];
            int hasRead = tableDataFile.read(data);
            return new HeapPage((HeapPageId) pid, data);
        }
        catch (IOException | ClassCastException e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset
     *             into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = 0;
        try {
            num = (int) (tableDataFile.length() / BufferPool.getPageSize());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        return null;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private final int totalPageNumber;
        private int currentPageNumber = 0;
        private Iterator<Tuple> iterator;
        private boolean isClose = false;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.totalPageNumber = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null || isClose) return false;
            if (iterator.hasNext()) return true;
            if (currentPageNumber >= totalPageNumber) return false;

            // Open a new Iterator<Tuple> here
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || isClose) {
                throw new NoSuchElementException("HeapFileIterator not open yet or close");
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (isClose) throw new NoSuchElementException("HeapFileIterator not open yet or close");
            currentPageNumber = 0;
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
        }

        @Override
        public void close() {
            isClose = true;
        }

        private Iterator<Tuple> getIterator(int pageNumber)
                throws TransactionAbortedException, DbException {
            if (pageNumber >= totalPageNumber) {
                throw new DbException("There is no Page not all");
            }
            // todo: Use READ_ONLY currently, dont know where to get it
            PageId pageId = new HeapPageId(getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            return page.iterator();
        }
    }

}

