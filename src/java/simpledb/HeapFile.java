package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile, Serializable{
    //added serializable according to specs in lab6

    File file;
    TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
       /** int pageSize = BufferPool.getPageSize();
        RandomAccessFile reader = null;
        byte[] buffer = new byte[BufferPool.getPageSize()];

        try {
            int from = pid.getPageNumber() * pageSize;
            reader = new RandomAccessFile(this.file, "r");
            if (from < reader.length()) {
                reader.seek(from);
                reader.read(buffer);
                reader.close();
            }
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buffer);
        }
        catch (IOException e) {
            return null;
        }*/




         RandomAccessFile retrieveFile;
        HeapPage page;
        byte[] pageData = new byte[BufferPool.getPageSize()];
        try {

            retrieveFile = new RandomAccessFile(this.file, "r");
            retrieveFile.seek(BufferPool.getPageSize() * pid.getPageNumber());
            retrieveFile.read(pageData);
            retrieveFile.close();
            page = new HeapPage((HeapPageId) pid, pageData);
            return page;

        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Something's wrong in HeapFile.readPage");
        }


        /**finally {
            if (retrieveFile != null){
                try {
                    retrieveFile.close();
                } catch (IOException e){
                    throw new IllegalArgumentException();
                }
            }
        }
        return page;*/
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile writeFile;
        try {
            writeFile = new RandomAccessFile(this.file, "rw");
            writeFile.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
            writeFile.write(page.getPageData());
            writeFile.close();
        } catch (Exception e){
            e.printStackTrace();
            throw new IllegalArgumentException("Something's wrong in HeapFile.writePage");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> ans = new ArrayList<>();
        HeapPage heapPage;

        for (int i = 0; i < this.numPages(); i++){
            PageId pageId = new HeapPageId(this.getId(), i);
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() != 0){
                heapPage.insertTuple(t);
                ans.add(heapPage);
                this.writePage(heapPage);
                break;
            }
        }

        if (ans.isEmpty()){
            heapPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
            t.setRecordId(new RecordId(new HeapPageId(getId(), numPages()), 0));
            heapPage.insertTuple(t);
            writePage(heapPage);
            ans.add(heapPage);
        }
        return ans;
    }

    // see DbFile.java for javadocs
    int count = 0;
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> ans = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        ans.add(heapPage);

        if (ans.isEmpty()){
            throw new DbException("can't find");
        }
        return ans;


    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HPIterator(tid, this);
    }

    @SuppressWarnings("checkstyle:CommentsIndentation")
    public static class HPIterator implements DbFileIterator, Serializable{
        //added serializable according to specs in lab6

        TransactionId transactionId;
        Iterator<Tuple> tupleIteratorInPg;
        HeapFile hf;
        int pageNum = 0;

        public void setPageNum(){
            this.pageNum = 0;
        }

        public void shiftPageNum(){
            this.pageNum++;
        }

        public HPIterator(TransactionId tid, HeapFile heapFile){
            this.transactionId = tid;
            this.hf = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            //int initaillPageNum = 0;

            this.setPageNum();
            HeapPage hp = (HeapPage) Database.getBufferPool().
                    getPage(this.transactionId, new HeapPageId(hf.getId(), this.pageNum),
                            Permissions.READ_ONLY);
            this.tupleIteratorInPg = hp.iterator(); //tpIterator(this.cursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.pageNum >= hf.numPages() || this.tupleIteratorInPg == null){
                return false;
            }
           // else {
                if (!this.tupleIteratorInPg.hasNext()) {
                    this.shiftPageNum();
                    if (pageNum >= hf.numPages()){
                        this.tupleIteratorInPg = null;
                        return false;
                    }

                    // worth take another look

                    HeapPage hp = (HeapPage) Database.getBufferPool().
                                getPage(this.transactionId, new HeapPageId(hf.getId(), this.pageNum),
                                        Permissions.READ_ONLY);
                    this.tupleIteratorInPg = hp.iterator();
                        //return (this.tupleIteratorInPg != null && this.tupleIteratorInPg.hasNext());

                }
                return true;
          //  }

           /** while (!this.tupleIteratorInPg.hasNext()){
                this.shiftCursor();
                if (cursor >= hf.numPages()){
                    return false;
                }
                this.tupleIteratorInPg = tpIterator(cursor);
            }*/
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("Oops! no next!");
            }

                Tuple tuple = this.tupleIteratorInPg.next();
                //System.out.println(tuple + " has next? ->" + hasNext());
                return tuple;



            /**
             * if (this.tupleIteratorInPg == null) {
             *                 throw new NoSuchElementException("No more tuples!");
             *             }
             *             Tuple t = this.tupleIteratorInPg.next();
             *             return t;
             */

        }



        @Override
        public void rewind() throws DbException, TransactionAbortedException {

            this.close();
            this.open();
        }

        @Override
        public void close() {
            this.setPageNum();
            this.tupleIteratorInPg = null;
        }
    }
}

