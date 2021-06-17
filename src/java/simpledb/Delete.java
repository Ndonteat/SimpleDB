package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private boolean called;
    private TupleDesc tupleDesc;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.t = t;
        this.child = child;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
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
        this.called = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    //int count = 0;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.called){
            return null;
        }

        int cursor = 0;
        BufferPool bufferPool = Database.getBufferPool();
        while (this.child.hasNext()){
            //System.out.println("(Delete)Delete count " + this.count++);
            try {
                bufferPool.deleteTuple(this.t, this.child.next());
                cursor += 1;
            } catch (IOException e){
                e.printStackTrace();
            }
        }
        //System.out.println("Deleted number " + cursor);

        this.called = true;
        Tuple ans = new Tuple(this.getTupleDesc());
        ans.setField(0, new IntField(cursor));
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
