package simpledb.execution;

import java.io.IOException;

import simpledb.test;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
    private TransactionId transactionId;
    private OpIterator[] children = new OpIterator[1];
    private int tableId;
    private TupleDesc tabTupleDesc;

    private TupleDesc outTupleDesc;
    private Boolean rewind; //false -->begin ,

    private static final long serialVersionUID = 1L;

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
        this.transactionId = t;
        this.children[0] = child;
        this.tableId = tableId;
        this.outTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.rewind = false; 
        this.tabTupleDesc = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(tabTupleDesc) ) throw new DbException("TupleDesc of child differs from table");

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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 将从child读取的元组插入到构造函数指定的tableId中。
     * 它返回一个包含插入记录数的单字段元组。插入应通过BufferPool传递。
     * BufferPool的实例可通过Database.getBufferPool（）获得。
     * 请注意，insert不需要在插入特定元组之前检查它是否重复。
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple ans = new Tuple(outTupleDesc);
        int num =0;
        while (children[0].hasNext()){
            Tuple tuple = children[0].next();
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
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
