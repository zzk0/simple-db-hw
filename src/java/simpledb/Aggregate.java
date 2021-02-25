package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int aField;
    private final int gField;
    private final Aggregator.Op op;

    private Aggregator aggregator;
    private OpIterator iterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param aField
     *            The column over which we are computing an aggregate.
     * @param gField
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param op
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aField, int gField, Aggregator.Op op) {
	    // some code goes here
        this.child = child;
        this.aField = aField;
        this.gField = gField;
        this.op = op;
        initializeAggregator();
        this.iterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        if (gField >= 0 && gField < child.getTupleDesc().numFields()) {
            return gField;
        }
        return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if (gField >= 0 && gField < child.getTupleDesc().numFields()) {
            return child.getTupleDesc().getFieldName(gField);
        }
        return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        if (aField >= 0 && aField < child.getTupleDesc().numFields()) {
            return child.getTupleDesc().getFieldName(aField);
        }
        return null;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // some code goes here
        super.open();
        iterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        return child.getTupleDesc();
    }

    public void close() {
	    // some code goes here
        iterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // some code goes here
        assert children.length > 0;
        child = children[0];
        initializeAggregator();
    }

    private void initializeAggregator() {
        Type gFieldType = null;
        if (gField != Aggregator.NO_GROUPING) {
            gFieldType = child.getTupleDesc().getFieldType(gField);
        }
        switch (child.getTupleDesc().getFieldType(aField)) {
            case INT_TYPE:
                aggregator = new IntegerAggregator(gField, gFieldType, aField, op);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(gField, gFieldType, aField, op);
                break;
        }

        try {
            child.open();
            while (child.hasNext()) {
                Tuple tuple = child.next();
                aggregator.mergeTupleIntoGroup(tuple);
            }
            child.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
