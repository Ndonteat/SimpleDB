package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    TransactionId transactionId;
    int tableId;
    String tableName;
    String alias;
    DbFileIterator dbFileIterator;
    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.transactionId = tid;
        this.tableId = tableid;
        this.alias = tableAlias;
        this.tableName = Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {

//        String name = Database.getCatalog().getTableName(this.tableId);
        return this.tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.alias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.dbFileIterator = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.transactionId);
        this.dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc();
        String[] names = new String[td.numFields()];
        Type[] types = new Type[td.numFields()];
        int i = 0;
        while (i < td.numFields()){
            names[i] = this.alias + "." + td.getFieldName(i);
            types[i] = td.getFieldType(i);
            i++;
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (dbFileIterator == null){
            return false;
        }
        else {
            return dbFileIterator.hasNext();
        }
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!dbFileIterator.hasNext()){
            throw new NoSuchElementException("Some thing's wrong in SeqScan.next()");
        }
        else {
            return dbFileIterator.next();
        }
    }

    public void close() {
        // some code goes here
        dbFileIterator.close();
//        dbFileIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        dbFileIterator.rewind();
    }
}
