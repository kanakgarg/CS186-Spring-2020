package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.common.Pair;

public class LoggingLockContext extends LockContext {
    private boolean allowDisable = true;

    /**
     * A special LockContext that works with a LoggingLockManager to emit logs
     * when the user uses disableChildLocks() or capacity()
     */
    LoggingLockContext(LoggingLockManager lockman, LockContext parent, Pair<String, Long> name) {
        super(lockman, parent, name);
    }

    private LoggingLockContext(LoggingLockManager lockman, LockContext parent, Pair<String, Long> name,
                               boolean readonly) {
        super(lockman, parent, name, readonly);
    }

    /**
     * Disables locking children. This causes all child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    @Override
    public synchronized void disableChildLocks() {
        if (this.allowDisable) {
            super.disableChildLocks();
        }
        ((LoggingLockManager) lockman).emit("disable-children " + name);
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    @Override
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LoggingLockContext((LoggingLockManager) lockman, this, new Pair<>(readable,
                name),
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        return child;
    }

    /**
     * Sets the capacity (number of children).
     */
    @Override
    public synchronized void capacity(int capacity) {
        int oldCapacity = super.capacity;
        super.capacity(capacity);
        if (oldCapacity != capacity) {
            ((LoggingLockManager) lockman).emit("set-capacity " + name + " " + capacity);
        }
    }

    public synchronized void allowDisableChildLocks(boolean allow) {
        this.allowDisable = allow;
    }
}

