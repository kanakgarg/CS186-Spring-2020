package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == NL || b == NL){
            return true;
        }
        if (a.equals(IS)) {
            return !b.equals(X);
        }
        if (b.equals(IS)) {
            return !a.equals(X);
        }
        if (a.equals(IX) || b.equals(IX)) {
            return a.equals(b);
        }
        if (a.equals(S) || b.equals(S)) {
            return a.equals(b);
        }
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        switch (childLockType) {
            case NL:
                return true;
            case IS:
                return parentLockType == IS || parentLockType == IX;
            case S:
                return parentLockType == IS || parentLockType == IX;
            case IX:
                return parentLockType == IX || parentLockType == SIX;
            case X:
                return parentLockType == IX || parentLockType == SIX;
            case SIX:
                return parentLockType == IX;
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (substitute.equals(required)) {
            return true;
        }

        if(required.equals(NL)){
            return true;
        }
        if (required.equals(S)) {
            if (substitute.equals(X) || substitute.equals(SIX)) {
                return true;
            }
            else {
                return false;
            }
        }
        if (required.equals(IX)) {
            if (substitute.equals(SIX)) {
                return true;
            }
            else {
                return false;
            }
        }
        if (required.equals(IS)) {
            if (substitute.equals(IX) || substitute.equals(SIX)) {
                return true;
            }
            else {
                return false;
            }
        }
        if (required.equals(X) || required.equals(SIX)) {
            return false;
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

