package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gField;
    private final Type gFieldType;
    private final int aField;
    private final Op op;

    private static class DoubleInteger {
        public Integer first;
        public Integer second;

        public DoubleInteger(Integer first, Integer second) {
            this.first = first;
            this.second = second;
        }
    }
    private final Map<Field, DoubleInteger> map;

    /**
     * Aggregate constructor
     * 
     * @param gField
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aField
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gField, Type gFieldType, int aField, Op what) {
        // some code goes here
        this.gField = gField;
        this.gFieldType = gFieldType;
        this.aField = aField;
        this.op = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggregateField = tup.getField(aField);
        if (aggregateField.getClass() != IntField.class) {
            throw new IllegalArgumentException("It is not an IntField");
        }
        int value = ((IntField) aggregateField).getValue();

        // default field
        Field groupByField = new IntField(0);
        if (gField != NO_GROUPING) {
            groupByField = tup.getField(gField);
        }
        DoubleInteger doubleInteger = map.get(groupByField);
        if (doubleInteger == null) {
            doubleInteger = new DoubleInteger(value, 0);
        }

        switch (op) {
            case MIN:
                if (doubleInteger.first > value) {
                    doubleInteger.first = value;
                }
                break;
            case MAX:
                if (doubleInteger.first < value) {
                    doubleInteger.first = value;
                }
                break;
            case COUNT:
                // use second to count
                doubleInteger.second++;
                break;
            case SUM:
            case AVG:
            case SUM_COUNT:
            case SC_AVG:
                if (doubleInteger.second != 0) {
                    doubleInteger.first += value;
                }
                doubleInteger.second++;
                break;
        }

        map.put(groupByField, doubleInteger);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        final TupleDesc desc = TupleDesc.createAggregatorTupleDesc(
                "AggregateProperty", gFieldType, gField);

        return new OpIterator() {
            boolean isClosed = true;
            Iterator<Map.Entry<Field, DoubleInteger>> iterator;

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
                Map.Entry<Field, DoubleInteger> entry = iterator.next();
                Tuple tuple = new Tuple(desc);
                int index = 0;
                if (gField != NO_GROUPING) {
                    tuple.setField(index++, entry.getKey());
                }
                switch (op) {
                    case MIN:
                    case MAX:
                    case SUM:
                        tuple.setField(index, new IntField(entry.getValue().first));
                        break;
                    case AVG:
                        tuple.setField(index,
                                new IntField(entry.getValue().first / entry.getValue().second));
                        break;
                    case COUNT:
                        tuple.setField(index, new IntField(entry.getValue().second));
                        break;
                    case SUM_COUNT:
                        break;
                    case SC_AVG:
                        break;
                }
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
