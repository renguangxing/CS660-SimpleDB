package simpledb;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	private static TransactionId NO_LOCK = new TransactionId();
	private ConcurrentHashMap<PageId, Object> lockMap;
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> sharedlockMap;
	private ConcurrentHashMap<PageId, TransactionId> exclusivelockMap;
	private ConcurrentHashMap<TransactionId, HashSet<PageId>> transactionPageMap;
	private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> dependencyMap;

	public LockManager() {
		this.lockMap = new ConcurrentHashMap<PageId, Object>();
		this.sharedlockMap = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		this.exclusivelockMap = new ConcurrentHashMap<PageId, TransactionId>();
		this.transactionPageMap = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
		this.dependencyMap = new ConcurrentHashMap<TransactionId, HashSet<TransactionId>>();
	}

	


	public void getLock(TransactionId tid, PageId pid, Permissions p)
		throws TransactionAbortedException {

		if (!(this.dependencyMap.containsKey(tid))) {
			this.dependencyMap.put(tid, new HashSet<TransactionId>());
		}

		Object lock = this.getLock(pid);
		if ((p == Permissions.READ_ONLY) &&
				!(this.sharedlockMap.get(pid).contains(tid))) {
			while (true) {
				synchronized(lock) {
					if (this.exclusivelockMap.get(pid).equals(NO_LOCK) ||
							this.exclusivelockMap.get(pid).equals(tid)) {
						synchronized(this.sharedlockMap.get(pid)) {
							this.sharedlockMap.get(pid).add(tid);
						}

						synchronized(this.dependencyMap) {
							this.dependencyMap.remove(tid);
						}

						break;
					}

					// Check for confilt-serializable.
					synchronized(this.dependencyMap) {
						if (this.dependencyMap.get(tid).add(this.exclusivelockMap.get(pid))) {
							if (this.isConfiltSerializable(tid)) {
								throw new TransactionAbortedException();
							}
						}
					}
				}
			}
		} else if ((p == Permissions.READ_WRITE) && !(this.exclusivelockMap.get(pid).equals(tid))) {
			while (true) {
				synchronized(lock) {
					HashSet<TransactionId> depMap = new HashSet<TransactionId>();
					if (!(this.exclusivelockMap.get(pid).equals(NO_LOCK))) {
						depMap.add(this.exclusivelockMap.get(pid));
					}

					synchronized(this.sharedlockMap.get(pid)) {
						Iterator<TransactionId> it = this.sharedlockMap.get(pid).iterator();
						while (it.hasNext()) {
							TransactionId t = it.next();
							if (!(t.equals(tid))) {
								depMap.add(t);
							}
						}
					}
					
					
					if (depMap.isEmpty()) {
						// Remove the shared lock if current page holds.
						synchronized(this.sharedlockMap.get(pid)) {
							this.sharedlockMap.get(pid).remove(tid);
						}

						this.exclusivelockMap.put(pid, tid);

						synchronized(this.dependencyMap) {
							this.dependencyMap.remove(tid);
						}

						break;
					}

					// Check confilt-serializable.
					synchronized(this.dependencyMap) {
						if (this.dependencyMap.get(tid).add(this.exclusivelockMap.get(pid)) ||
							this.dependencyMap.get(tid).addAll(depMap)) {
							if (this.isConfiltSerializable(tid)) {
								throw new TransactionAbortedException();
							}
						}
					}
				}
			}
		}
		
		if (!(this.transactionPageMap.containsKey(tid))) {
			this.transactionPageMap.put(tid, new HashSet<PageId>());
		}
		
		synchronized(this.transactionPageMap.get(tid)) {
			this.transactionPageMap.get(tid).add(pid);
		}
	}

	public void releaseLock(TransactionId tid, PageId pid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return;
		}

		Object lock = this.getLock(pid);
		synchronized(lock) {
			if (this.exclusivelockMap.get(pid).equals(tid)) {
				this.exclusivelockMap.put(pid, NO_LOCK);
			}
	
			synchronized(this.sharedlockMap.get(pid)) {
				this.sharedlockMap.get(pid).remove(tid);
			}
		}

		synchronized(this.transactionPageMap.get(tid)) {
			this.transactionPageMap.get(tid).remove(pid);
		}
	}

	public void releaseAllLocks(TransactionId tid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return;
		}

		Iterator<PageId> it = this.getPages(tid);
		while (it.hasNext()) {
			PageId pid = it.next();

			Object lock = this.getLock(pid);
			synchronized(lock) {
				if (this.exclusivelockMap.get(pid).equals(tid)) {
					this.exclusivelockMap.put(pid, NO_LOCK);
				}
	
				synchronized(this.sharedlockMap.get(pid)) {
					this.sharedlockMap.get(pid).remove(tid);
				}
			}
		}

		this.transactionPageMap.remove(tid);
	}
	
	private Object getLock(PageId pid) {
		if (!(this.lockMap.containsKey(pid))) {
			this.lockMap.put(pid, new Object());
			this.sharedlockMap.put(pid, new HashSet<TransactionId>());
			this.exclusivelockMap.put(pid, NO_LOCK);
		}

		return this.lockMap.get(pid);
	}
	

	public Iterator<PageId> getPages(TransactionId tid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return null;
		}

		return this.transactionPageMap.get(tid).iterator();
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		if (!(this.transactionPageMap.containsKey(tid))) {
			return false;
		}

		synchronized(this.transactionPageMap.get(tid)) {
			return this.transactionPageMap.get(tid).contains(pid);
		}
	}
	

	// Run BFS to detect cycles.
	private boolean isConfiltSerializable(TransactionId tid) {
		HashSet<TransactionId> visitedTransaction = new HashSet<TransactionId>();
		LinkedList<TransactionId> queue = new LinkedList<TransactionId>();
		queue.add(tid);
		while (!(queue.isEmpty())) {
			TransactionId curTra = queue.remove();
			if (visitedTransaction.contains(curTra)) return true;
			visitedTransaction.add(curTra);
			if (this.dependencyMap.containsKey(curTra) && !(this.dependencyMap.get(curTra).isEmpty())) {
				Iterator<TransactionId> itor = this.dependencyMap.get(curTra).iterator();
				while (itor.hasNext()) {
					queue.add(itor.next());
				}
			}
		}

		return false;
	}
}