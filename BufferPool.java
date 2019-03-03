
package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> cache;
    private LockManager LM;
    private int numPages;
    private PageId usedID = null;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.cache = new ConcurrentHashMap<PageId, Page>();
        this.LM = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (this.cache.size() >= this.numPages) {
        	this.evictPage();
        }

        if (!(this.cache.containsKey(pid))) {
        	Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        	p.setBeforeImage();
	        this.cache.put(pid, p);
        }

        this.usedID = pid;
        this.LM.getLock(tid, pid, perm);
        return this.cache.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        this.LM.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.LM.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if (this.LM.getPages(tid) == null) {
    		return;
    	}

        if (commit) {
            Iterator<PageId> itor = this.LM.getPages(tid);
            while (itor.hasNext()) {
            	PageId pid = itor.next();
            	Page p = this.cache.get(pid);
            	if (p != null) {
                	this.flushPage(pid);
                	this.cache.get(pid).setBeforeImage();
            	}
            }
        } else {
        	Iterator<PageId> itor = this.LM.getPages(tid);
        	while (itor.hasNext()) {
        		PageId pid = itor.next();
        		if (this.cache.containsKey(pid)) {
	        		Page p = this.cache.get(pid);
	        		if (p.isDirty() != null &&
	        			p.isDirty().equals(tid)) {
	        			this.cache.put(pid, p.getBeforeImage());
	        		}
        		}
        	}
        }

        this.LM.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> changed = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
    	Iterator<Page> itor = changed.iterator();
    	while (itor.hasNext()) {
    		Page p = itor.next();
    		p.markDirty(true, tid);
    		this.cache.put(p.getId(), p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markPageDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> changed =Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    	Iterator<Page> itor = changed.iterator();
    	while (itor.hasNext()) {
    		Page p = itor.next();
    		p.markDirty(true, tid);
    		this.cache.put(p.getId(), p);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> pageIds = this.cache.keySet().iterator();
        while (pageIds.hasNext()) {
        	this.flushPage(pageIds.next());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Page p = this.cache.get(pid);
    	if (p != null) {
	    	TransactionId tid = p.isDirty();
	    	if (tid != null) {
	    		Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
	    	    Database.getLogFile().force();
	    	}

    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(this.cache.get(pid));

        	p.markDirty(false, tid);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<PageId> itor = this.LM.getPages(tid);
        while (itor.hasNext()) {
        	this.flushPage(itor.next());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Only evicts dirty pages as part of the NO STEAL/FORCE policy.
     */
    private synchronized void evictPage() throws DbException {
    	PageId evictPid = this.usedID;
    	Iterator<PageId> itor = this.cache.keySet().iterator();
		while (itor.hasNext() && (this.cache.get(evictPid).isDirty() != null)) {
			evictPid = itor.next();
		}
		
		if (this.cache.get(evictPid).isDirty() != null) {
			throw new DbException("Cannot evict a page because all pages are dirty.");
		}

        this.discardPage(evictPid);
    }

}