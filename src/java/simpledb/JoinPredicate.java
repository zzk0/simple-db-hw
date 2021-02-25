package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int fieldId1;
    private final Predicate.Op op;
    private final int fieldId2;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.fieldId1 = field1;
        this.op = op;
        this.fieldId2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        if (t1 == null || t2 == null) return false;
        Field fieldValue1 = t1.getField(fieldId1);
        Field fieldValue2 = t2.getField(fieldId2);
        return fieldValue1.compare(op, fieldValue2);
    }
    
    public int getField1()
    {
        // some code goes here
        return fieldId1;
    }
    
    public int getField2()
    {
        // some code goes here
        return fieldId2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
