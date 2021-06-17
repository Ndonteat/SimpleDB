package simpledb.parallel;

import org.apache.mina.core.session.IoSession;
import simpledb.*;
import simpledb.OpIterator;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some
 * partition function (provided as a PartitionFunction object during the
 * ShuffleProducer's instantiation).
 * 
 * */
public class ShuffleProducer extends Producer {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private ParallelOperatorID operatorID;
    private SocketInfo[] workers;
    private PartitionFunction<?, ?> pf;

    public String getName() {
        return "shuffle_p";
    }

    public ShuffleProducer(OpIterator child, ParallelOperatorID operatorID,
                           SocketInfo[] workers, PartitionFunction<?, ?> pf) {
        super(operatorID);
        // some code goes here
        this.child = child;
        this.operatorID = operatorID;
        this.workers = workers;
        this.pf = pf;
    }

    public void setPartitionFunction(PartitionFunction<?, ?> pf) {
        // some code goes here
        this.pf = pf;
    }

    public SocketInfo[] getWorkers() {
        // some code goes here
        return this.workers;
    }

    public PartitionFunction<?, ?> getPartitionFunction() {
        // some code goes here
        return this.pf;
    }

    private void partitionSessions(IoSession[] sessions) throws DbException, TransactionAbortedException {
        Tuple tuple = child.next();
        TupleDesc tupleDesc = child.getTupleDesc();
        int partition = pf.partition(tuple, tupleDesc);
        sessions[partition].write(new TupleBag(operatorID, getThisWorker().workerID,
                new Tuple[]{tuple}, getTupleDesc()));
    }


    private void creatSessions(IoSession[] sessions){
        int j = 0;
        while (j < sessions.length){
            sessions[j] = ParallelUtility.createSession(
                    workers[j].getAddress(), getThisWorker().minaHandler, -1);
            j++;
        }
    }

    private void writeSessions(IoSession[] sessions){
        int j = 0;
        while (j < sessions.length){
            sessions[j].write(new TupleBag(operatorID, null));
            j++;
        }
    }

    // some code goes here
    class WorkingThread extends Thread {
            // some code goes here
            public void run(){
                IoSession[] sessions = new IoSession[workers.length];
                creatSessions(sessions);
                try {
                    while (child.hasNext()) {
                        partitionSessions(sessions);
                    }
                    writeSessions(sessions);

                } catch (TransactionAbortedException | DbException e) {
                    e.printStackTrace();
                }

            }

    }

    private WorkingThread workingThread;

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        workingThread = new WorkingThread();
        super.open();
        child.open();
        workingThread.start();
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            workingThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        this.child = children[0];

    }
}
