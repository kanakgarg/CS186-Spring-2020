package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import java.util.Deque;
import java.util.ArrayDeque;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        //ensure type of lock
        if (!lockType.equals(LockType.S) && !lockType.equals(LockType.X)) {
            return;
        }

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        Deque<LockContext> stack = new ArrayDeque<>();

        //transaction must not be null
        if (transaction == null) {
            return;
        }
        else {



            for(LockContext val = lockContext.parentContext(); val != null; val = val.parentContext()){
                stack.addFirst(val);
            }

            LockType LT = LockType.parentLock(lockType);

            while (stack.size() > 0) {
                LockContext LC = stack.removeFirst();
                if (LockType.substitutable(LT, LC.getEffectiveLockType(transaction))) {
                    helper(transaction, LC, LT);
                }
            }


            if (lockContext.numChildLocks.containsKey(transaction.getTransNum()) && lockContext.numChildLocks.get(transaction.getTransNum()) > 0 ) {
                lockContext.escalate(transaction);
            } else {
                helper(transaction, lockContext, lockType);
            }

        }


    }

    // TODO(proj4_part2): add helper methods as you see fit

    public static void helper(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        try {
            lockContext.promote(transaction, lockType);
        } catch (Exception e) {
            try {
                lockContext.acquire(transaction, lockType);
            } catch (Exception f) {

            }
        }
    }
}
