package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    int field1;
    int filed2;
    Predicate.Op op;

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
        this.op = op;
        this.field1 = field1;
        this.filed2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Field t1Filed = t1.getField(this.field1);
        Field t2Filed = t2.getField(this.filed2);
        return t1Filed.compare(this.op, t2Filed);
        //use the compare function of the secified Field in t1 as the compare function
        //use the op in the constructor as the operation the compare function gonna use
        //use the Field of t2 as the Field t1's Field gonna be compared to
    }
    
    public int getField1() //looks like have to use getField(), but the return type is int, so...
    {
        // some code goes here
        return this.field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return this.filed2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return this.op;
    }
}
