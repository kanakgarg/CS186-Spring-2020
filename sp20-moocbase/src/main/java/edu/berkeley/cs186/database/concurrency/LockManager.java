package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            for (Lock l : locks) {
                if (!LockType.compatible(l.lockType, lockType)) {
                    if(l.transactionNum == except) {
                        continue;
                    }
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            long tN = lock.transactionNum;

            if (transactionLocks.containsKey(tN)) {

                if (transactionLocks.get(tN).isEmpty()) {
                    transactionLocks.get(tN).add(lock);
                } else {
                    boolean b = false;
                    List<Lock> list = new ArrayList<>();
                    for (Lock l : transactionLocks.get(tN)) {
                        if (l.name == lock.name) {
                            b = true;
                            l.lockType = lock.lockType;
                        }
                        if (!b) {
                            list.add(lock);
                            b = true;
                        }
                    }
                    transactionLocks.get(tN).addAll(list);
                }
            } else {

                transactionLocks.put(tN, new ArrayList<>());
                transactionLocks.get(tN).add(lock);
            }


            boolean bc = false;
            for (Lock lk : this.locks) {
                if (lk.transactionNum == tN) {
                    bc = true;
                }
            }

            if (!bc) {
                this.locks.add(lock);
            }
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            if( lock == null){
                return;
            }
            transactionLocks.get(lock.transactionNum).remove(lock);
            this.locks.remove(lock);

            processQueue();
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                this.waitingQueue.addFirst(request);
            } else {
                this.waitingQueue.addLast(request);
            }
            request.transaction.prepareBlock();
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement

            while (!this.waitingQueue.isEmpty()) {
                LockRequest first = this.waitingQueue.getFirst();
                long transNum = first.lock.transactionNum;
                if (checkCompatible(first.lock.lockType, transNum)) {
                    waitingQueue.removeFirst();


                    for (Lock r : first.releasedLocks) {
                        release(first.transaction, r.name);
                    }

                    grantOrUpdateLock(first.lock);
                    first.transaction.unblock();
                } else {
                    break;
                }
            }
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock l : this.locks) {
                if (l.transactionNum == transaction) {
                    return l.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean block = false;
        synchronized (this) {
            if (getLockType(transaction, name) == lockType) {
                throw new DuplicateLockRequestException("duplicate lock");
            }


            //for each RN on the release locks, it must match up with the locks on the transaction,
            boolean nlhe = true;
            List<Lock> locks = getLocks(transaction);
            List<Lock> releasedLocks = new ArrayList<>();

            for (ResourceName rname : releaseLocks) {
                for (Lock l : locks) {
                    if(l.name.equals(rname)) {
                        nlhe = false;
                        releasedLocks.add(l);
                    }
                }
                if(nlhe) {
                    throw new NoLockHeldException("no lock");
                }
            }


            if (!this.resourceEntries.containsKey(name)) {
                this.resourceEntries.put(name, getResourceEntry(name));
            }

            ResourceEntry re = resourceEntries.get(name);

            //lock to add
            Lock nLock = new Lock(name, lockType, transaction.getTransNum());

            // check if requested lock is not compatible with another lock on the resource
            if(!re.checkCompatible(lockType, transaction.getTransNum())) {
                re.addToQueue(new LockRequest(transaction, nLock, releasedLocks), true);
                block = true;
            } else {
                re.grantOrUpdateLock(nLock);
                for( ResourceName rname : releaseLocks) {
                    if(!name.equals(rname)) {
                        release(transaction, rname);
                    }
                }
            }
        }
        if (block) {
            transaction.block();
        }

    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.

        boolean block = false;

        if (getLockType(transaction, name) != LockType.NL) {
            throw new DuplicateLockRequestException("duplicate lock");
        }

        synchronized (this) {
                //add if first time in resource entry
                if (!resourceEntries.containsKey(name)) {
                    resourceEntries.put(name, getResourceEntry(name));
                }

                ResourceEntry re = resourceEntries.get(name);

                //lock to add
                Lock nLock = new Lock(name, lockType, transaction.getTransNum());

                // check if requested lock is not compatible with another lock on the resource
                if(!re.checkCompatible(lockType, transaction.getTransNum()) || !re.waitingQueue.isEmpty()) {
                    re.addToQueue(new LockRequest(transaction, nLock), false);
                    block = true;
                } else {
                    re.grantOrUpdateLock(nLock);
                }
        }

        if(block){
            transaction.block();
        }

    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        if (!resourceEntries.containsKey(name)) {
            throw new NoLockHeldException("no locks for resource");
        }

        Lock l= null;
        ResourceEntry re = resourceEntries.get(name);
        synchronized (this) {

            boolean removed = false;

            for (Lock rem: re.locks) {
                if (transaction.getTransNum() == rem.transactionNum) {
                    l = rem;
                    removed = true;
                }
            }

            if (!removed) {
                throw new NoLockHeldException("no lock on name");
            }
        }

        re.releaseLock(l);
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.

        if (!transactionLocks.containsKey(transaction.getTransNum())) {
            throw new NoLockHeldException("transaction has no locks");
        }

        boolean block = false;
        Lock original = null;
        List<Lock> t = transactionLocks.get(transaction.getTransNum());
        synchronized (this) {

            boolean pointer = true;
            for (Lock l: t) {
                if(l.name.equals(name)) {
                    pointer = false;
                    original = l;
                }
            }
            if(pointer) {
                throw new NoLockHeldException("no locks on this resource to promote");
            }
            if (original.lockType == (newLockType)) {
                throw new DuplicateLockRequestException("lock with type already exists");
            }
            if (!LockType.substitutable(newLockType, original.lockType)) {
                throw new InvalidLockException("not substitutable");
            }
            if(!resourceEntries.containsKey(name)) {
                throw new NoLockHeldException("no locks on this resource to promote");
            }

            ResourceEntry re = resourceEntries.get(name);
            Lock nLock = new Lock(name, newLockType, transaction.getTransNum());
            if(re.checkCompatible(newLockType, transaction.getTransNum())){
                re.grantOrUpdateLock(nLock);
            } else{
                re.addToQueue(new LockRequest(transaction, nLock), true);
                block = true;
            }
        }

        if (block) {
            transaction.block();
        }


    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        if(!resourceEntries.containsKey(name)){
            return LockType.NL;
        }
        ResourceEntry locks = resourceEntries.get(name);

        return locks.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
