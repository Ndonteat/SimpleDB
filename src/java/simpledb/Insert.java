package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId t;
    OpIterator child;
    int tableId;
    int called = 0;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("TupleDesc mismatch");
        }

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] t = new Type[] {Type.INT_TYPE};
        String[] s = new String[] {"numOfInserted"};
        return new TupleDesc(t, s);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.called != 0){
            return null;
        }
        int cursor = 0;
        this.called += 1;

        while (this.child.hasNext()){
            try {
                cursor += 1;
                Database.getBufferPool().insertTuple(this.t, this.tableId, this.child.next());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        Tuple ans = new Tuple(this.getTupleDesc());
        IntField intField = new IntField(cursor);
        ans.setField(0, intField);
        return ans;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
