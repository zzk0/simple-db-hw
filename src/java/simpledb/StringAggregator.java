package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gField;
    private final Type gFieldType;
    private final int aField;
    private final Op op;

    // Since we only support Op.COUNT, store an Integer is fine
    private final Map<Field, Integer> map;

    /**
     * Aggregate constructor
     * @param gField the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aField the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gField, Type gFieldType, int aField, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("STRING_TYPE supports Op.COUNT only");
        }
        this.gField = gField;
        this.gFieldType = gFieldType;
        this.aField = aField;
        this.op = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // default field
        Field field = new IntField(0);
        if (gField != NO_GROUPING) {
            field = tup.getField(gField);
        }
        Integer count = map.get(field);
        if (count == null) {
            count = 0;
        }
        map.put(field, count + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        final TupleDesc desc = TupleDesc.createAggregatorTupleDesc(
                "AggregateProperty", gFieldType,  gField);

        return new OpIterator() {
            boolean isClosed = true;
            Iterator<Map.Entry<Field, Integer>> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (!isClosed) {
                    throw new IllegalStateException("Iterator was opened");
                }
                iterator = map.entrySet().iterator();
                isClosed = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException,
                    NoSuchElementException {
                if (!iterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                Map.Entry<Field, Integer> entry = iterator.next();
                Tuple tuple = new Tuple(desc);
                int index = 0;
                if (gField != NO_GROUPING) {
                    tuple.setField(index++, entry.getKey());
                }
                tuple.setField(index, new IntField(entry.getValue()));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iterator = map.entrySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return desc;
            }

            @Override
            public void close() {
                if (isClosed) {
                    throw new IllegalStateException("Iterator was closed");
                }
                iterator = null;
                isClosed = true;
            }
        };
    }

}
