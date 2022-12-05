package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
    private TransactionId transactionId;
    private OpIterator[] children = new OpIterator[1];
    private TupleDesc outTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    
    private Boolean rewind; //false -->begin ,

    private static final long serialVersionUID = 1L;

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
        this.transactionId = t;
        this.children[0] = child;
        this.rewind = false; 
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return outTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        children[0].rewind();
        this.rewind =false;
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple ans = new Tuple(outTupleDesc);
        int num =0;
        while (children[0].hasNext()){
            Tuple tuple = children[0].next();
            try {
                Database.getBufferPool().deleteTuple(transactionId, tuple);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            num++;
        }
        ans.setField(0, new IntField(num) );
        if (rewind) return null;
        rewind = true;
        return ans;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
