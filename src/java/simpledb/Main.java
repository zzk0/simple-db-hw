package simpledb;

import java.io.File;

public class Main {

    public static void main(String[] args) throws Exception {
        // Create a schema, read file to build table
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] fieldNames = new String[] {
                "field0", "field1", "field2"
        };
        TupleDesc tupleDesc = new TupleDesc(types, fieldNames);
        HeapFile heapFile = new HeapFile(new File("data.dat"), tupleDesc);

        // add to Database
        Database.getCatalog().addTable(heapFile);

        // Let's iterate over it
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, heapFile.getId());
        scan.open();

        // Print header first and content
        System.out.println(scan.getTupleDesc());
        while (scan.hasNext()) {
            System.out.println(scan.next());
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }
}
