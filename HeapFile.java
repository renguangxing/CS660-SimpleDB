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
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	private int id;
	private File f;
	private TupleDesc td;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	this.id = f.getAbsoluteFile().hashCode();
    	
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
			RandomAccessFile file = new RandomAccessFile(f,"r");
			int offset = BufferPool.getPageSize() * pid.pageNumber();
			byte[] data = new byte[BufferPool.getPageSize()];
			if (offset + BufferPool.getPageSize() > file.length()) {
				System.err.println("page offset exceeds max size");
				System.exit(1);
            }
			file.seek(offset);
            file.readFully(data);
            file.close();
            return new HeapPage((HeapPageId)pid, data);
		} catch (FileNotFoundException e) {
			System.err.println("File not found");
			e.printStackTrace();
			throw new IllegalArgumentException();
		}catch (IOException e) {
			System.err.println("IO Exception");
			throw new IllegalArgumentException();
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    try {
    	RandomAccessFile rf = new RandomAccessFile(this.f,"rw");
    	rf.seek((long)page.getId().pageNumber()*BufferPool.getPageSize());
    	rf.write(page.getPageData(), 0, BufferPool.getPageSize());
    	rf.close();
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	HeapPage page = null;
    	int pageNo = 0;
    	while(pageNo < this.numPages()) {
    		page = (HeapPage) Database.getBufferPool().getPage(tid, 
    				new HeapPageId(this.getId(), pageNo), Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) break;
    		Database.getBufferPool().releasePage(tid, page.getId());
        	pageNo++;
    	}
    	
    	
    	page = new HeapPage(new HeapPageId(this.getId(),pageNo), HeapPage.createEmptyPageData());
    	this.writePage(page);
    	page = (HeapPage)Database.getBufferPool().getPage(tid, 
    			new HeapPageId(this.getId(),pageNo),Permissions.READ_WRITE);
    	
    	page.insertTuple(t);
    	ArrayList<Page> res = new ArrayList<Page>();
    	res.add(page);
    	return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
    			t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> res = new ArrayList<Page>();
        res.add(page);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
    
    private class HeapFileIterator extends AbstractDbFileIterator{
    	Iterator<Tuple> tupleIt;
  	  	int curPageNo;
  	  	TransactionId tid;
  	  	HeapFile hf;
  	  
  	  	public HeapFileIterator(HeapFile hf, TransactionId tid) {
  	  		this.hf = hf;
  	  		this.tid = tid;
  	  	}
  	  
  	  	public void open() throws DbException, TransactionAbortedException {
  	  		curPageNo = -1;
  	  	}
  	  
  	  	@Override
  	  	protected Tuple readNext() throws TransactionAbortedException, DbException{
  	  		if (tupleIt != null && !tupleIt.hasNext()) tupleIt = null;
  	  		while(tupleIt == null && curPageNo < this.hf.numPages() - 1) {
  	  			HeapPageId  curPageId = new HeapPageId(this.hf.getId(), ++curPageNo);
  	  			HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid,curPageId,Permissions.READ_ONLY);
  	  			tupleIt = curPage.iterator();
  	  			if (!tupleIt.hasNext()) tupleIt = null;
  	  		}
  	  		if (tupleIt == null) return null;
  	  		return tupleIt.next();
  	  	}

  	  	@Override
  	  	public void rewind() throws DbException, TransactionAbortedException {
  	  		close();
  	  		open();
		
  	  	}
  	  
  	  	public void close() {
  	  		super.close();
  	  		tupleIt = null;
  	  		curPageNo = Integer.MAX_VALUE;
  	  	}
  	  	
  	  	
  	  
  	  
  	
  	  
    }
    

}

