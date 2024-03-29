package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    private final HeapPageId pid;
    private final TupleDesc tupleDesc;
    private final byte[] header;
    private final Tuple[] tuples;
    private final int numSlots; // one slot per tuple
    
    private TransactionId transactionIdMakeDirty;
    private byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * used to count bit
     */
    private static final byte[] bitCount = new byte[1 << 8];

    static {
        int mask = 1;
        for (int i = 0; i < 256; i++) {
            if ((i & mask) != 0) bitCount[i]++;
            if ((i & (mask << 1)) != 0) bitCount[i]++;
            if ((i & (mask << 2)) != 0) bitCount[i]++;
            if ((i & (mask << 3)) != 0) bitCount[i]++;
            if ((i & (mask << 4)) != 0) bitCount[i]++;
            if ((i & (mask << 5)) != 0) bitCount[i]++;
            if ((i & (mask << 6)) != 0) bitCount[i]++;
            if ((i & (mask << 7)) != 0) bitCount[i]++;
        }
    }

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        } catch (NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        // tuplesPerPage = floor((pageSize * 8) / (len * 8 + 1))
        double bitsPerPage = BufferPool.getPageSize() * 8.0;
        double bitsPerTuple = tupleDesc.getSize() * 8.0 + 1.0;
        int numTuples = (int) Math.floor(bitsPerPage / bitsPerTuple);
        return numTuples;
    }

    /**
     * Computes the number of bytes in the header of a page
     * in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page
     * in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(getNumTuples() / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(tupleDesc);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j< tupleDesc.numFields(); j++) {
                Field f = tupleDesc.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit
     * should be updated to reflect that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (t == null || !t.getTupleDesc().equals(tupleDesc)) {
            throw new DbException("delete tuple cannot be null or TupleDesc is mismatch");
        }
        int tupleNumber = t.getRecordId().getTupleNumber();
        if (tupleNumber >= tuples.length ||
                !isSlotUsed(tupleNumber) ||
                !t.equals(tuples[tupleNumber])) {
            throw new DbException("Not exist such tuple");
        }
        markSlotUsed(tupleNumber, false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (t == null || !t.getTupleDesc().equals(tupleDesc)) {
            throw new DbException("Insert tuple cannot be null or TupleDesc is mismatch");
        }
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                tuples[i] = t;
                markSlotUsed(i, true);
                PageId pageId = getId();
                RecordId recordId = new RecordId(pageId, i);
                t.setRecordId(recordId);
                return;
            }
        }
        throw new DbException("Page is full");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1
        if (dirty) {
            transactionIdMakeDirty = tid;
        }
        else {
            transactionIdMakeDirty = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	    // Not necessary for lab1
        return transactionIdMakeDirty;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int numFillSlots = 0;
        for (byte num : header) {
            if (num >= 0) {
                numFillSlots += bitCount[num]; // if num is positive
            }
            else {
                numFillSlots += bitCount[255 + num + 1]; // if num is negative
            }
        }
        return numSlots - numFillSlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // locate the byte
        int slotBitOnByte = (int) Math.floor(i / 8.0);
        i = i - slotBitOnByte * 8;
        return ((header[slotBitOnByte] & (1 << i)) != 0);
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int slotBitOnByte = (int) Math.floor(i / 8.0);
        i = i - slotBitOnByte * 8;
        if (value) {
            header[slotBitOnByte] |= (1 << i);
        }
        else {
            header[slotBitOnByte] &= ~(1 << i);
        }
    }

    /**
     * @return an iterator over all tuples on this page
     * (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new Iterator<Tuple>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                while (i < numSlots) {
                    if (isSlotUsed(i)) {
                        return true;
                    }
                    i++;
                }
                return false;
            }

            @Override
            public Tuple next() {
                i++;
                return tuples[i - 1];
            }
        };
    }

}

