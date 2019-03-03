package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});
    private boolean fetched = false;
    
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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
    	if (!(Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc()))) {
    		throw new DbException("TupleDesc from table and of child are different");
    	}
    	this.t = t;
    	this.child = child;
    	this.tableId = tableId;
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
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
    	if (this.fetched) return null;
    	int numInserted = 0;
    	try {
    		this.child.open();
    		while(this.child.hasNext()) {
    			Database.getBufferPool().insertTuple(this.t, this.tableId, child.next());
    			numInserted++;
    		}
    		this.child.close();
    	}catch(IOException e) {
    		e.printStackTrace();
    	}
    	this.fetched = true;
    	Tuple t = new Tuple(this.td);
    	t.setField(0, new IntField(numInserted));
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if (children.length < 1) throw new IllegalArgumentException("wroing # of elements for children");
    	this.child = children[0];
    }
}
