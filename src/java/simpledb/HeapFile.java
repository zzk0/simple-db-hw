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
        long startPos = BufferPool.getPageSize() * page.getId().getPageNumber();
        tableDataFile.seek(startPos);
        tableDataFile.write(page.getPageData());
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
        // find a page
        HeapPageId heapPageId = null;
        HeapPage heapPage = null;
        boolean noNewPage = false;
        for (int i = 0; i < numPages(); i++) {
            heapPageId = new HeapPageId(id, i);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() > 0) {
                noNewPage = true;
            }
        }

        // new page needed
        if (!noNewPage) {
            heapPageId = new HeapPageId(id, numPages());
            heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(heapPage);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        }

        // insert Tuple, the HeapPage will update RecordId automatically
        heapPage.insertTuple(t);
        // writePage(heapPage); // no need because the BufferPool can flush to the disk

        // The ArrayList contains the pages that were modified
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(heapPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page.getClass() != HeapPage.class) {
            throw new DbException("This is not a HeapPage");
        }
        HeapPage heapPage = (HeapPage) page;
        heapPage.deleteTuple(t);

        // The ArrayList contains the pages that were modified
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        // private final int totalPageNumber;
        private int currentPageNumber = 0;
        private Iterator<Tuple> iterator;
        private boolean isClosed = true;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            // this.totalPageNumber = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (!isClosed) throw new DbException("You cannot open twice");
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
            isClosed = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null || isClosed) return false;
            if (iterator.hasNext()) return true;
            if (currentPageNumber >= numPages()) return false;

            // Open a new Iterator<Tuple> here
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
            while (!iterator.hasNext()) {
                if (currentPageNumber >= numPages()) return false;
                iterator = getIterator(currentPageNumber);
                currentPageNumber++;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || isClosed) {
                throw new NoSuchElementException("HeapFileIterator not open yet or closed");
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (isClosed) throw new NoSuchElementException("HeapFileIterator not open yet or closed");
            currentPageNumber = 0;
            iterator = getIterator(currentPageNumber);
            currentPageNumber++;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        private Iterator<Tuple> getIterator(int pageNumber)
                throws TransactionAbortedException, DbException {
            if (pageNumber >= numPages()) {
                throw new DbException("There is no Page not all");
            }
            // todo: Use READ_ONLY currently, dont know where to get it
            PageId pageId = new HeapPageId(getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            return page.iterator();
        }
    }

}
