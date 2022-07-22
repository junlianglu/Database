package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

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

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        if(transaction!=null) {
            if(lockContext!=null) {
                LockType currEffectiveLockType = lockContext.getEffectiveLockType(transaction);
                if(lockType == LockType.S) {
                    if(currEffectiveLockType == LockType.S) {
                        //duplicate or redundant
                        return;
                    }else if(currEffectiveLockType == LockType.X) {
                        //cannot demote or redundant
                        return;
                    }
                    //there is no ancestor that is S or X or SIX
                    else if(currEffectiveLockType == LockType.IS) {
                        ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IS);
                        lockContext.escalate(transaction);
                    }else if(currEffectiveLockType == LockType.IX) {
                        //promote to SIX to satisfy both IX and S
                        ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                        lockContext.promote(transaction, LockType.SIX);
                    }else if(currEffectiveLockType == LockType.SIX) {
                        //cannot demote
                        return;
                    }else {
                        //there is no ancestor that is S or X or SIX
                        //there is no lock at current level
                        ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IS);
                        lockContext.acquire(transaction, LockType.S);
                    }
                }else if(lockType == LockType.X) {
                    if(currEffectiveLockType == LockType.S) {
                        ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                        if(lockContext.getExplicitLockType(transaction) == LockType.NL) {
                            lockContext.acquire(transaction, LockType.X);
                        }else {
                            lockContext.promote(transaction, LockType.X);
                        }
                    }else if(currEffectiveLockType == LockType.X) {
                        //duplicate
                        /*if(lockContext.getExplicitLockType(transaction) == LockType.X) {
                            return;
                        }
                        if()*/
                        //duplicate or redundant
                        return;
                    }
                    //there is no ancestor that is S or X or SIX
                    else if(currEffectiveLockType == LockType.IS) {
                        lockContext.escalate(transaction);
                        if(lockContext.getExplicitLockType(transaction) != LockType.X) {
                            ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                            lockContext.promote(transaction, LockType.X);
                        }
                    }else if(currEffectiveLockType == LockType.IX) {
                        lockContext.escalate(transaction);
                        if(lockContext.getExplicitLockType(transaction) != LockType.X) {
                            ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                            lockContext.promote(transaction, LockType.X);
                        }
                    }else if(currEffectiveLockType == LockType.SIX) {
                        lockContext.escalate(transaction);
                        if(lockContext.getExplicitLockType(transaction) != LockType.X) {
                            ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                            lockContext.promote(transaction, LockType.X);
                        }
                    }else {
                        //there is no ancestor that is X
                        //there is no lock at current level
                        ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                        lockContext.acquire(transaction, LockType.X);
                    }
                }else if(lockType == LockType.IX) {
                    //special case for Database#lockTableMetadata mayNeedToCreate is true
                    ensureSufficientParentLockHeld(lockContext.parentContext(), LockType.IX);
                    if(lockContext.getExplicitLockType(transaction) != LockType.NL) {
                        if(lockContext.getExplicitLockType(transaction) == LockType.IX) {
                            return;
                        }else if(LockType.substitutable(LockType.IX, lockContext.getExplicitLockType(transaction))) {
                            lockContext.promote(transaction, LockType.IX);
                        }else {
                            // cannot demote
                        }
                    }else {
                        lockContext.acquire(transaction, LockType.IX);
                    }
                }
            }
        }
    }

    // TODO(proj4_part2): add helper methods as you see fit
    private static void ensureSufficientParentLockHeld(LockContext lockContext, LockType lockType_IS_or_IX) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if(lockContext == null) {
            return;
        }else {
            LockType currLockType = lockContext.getExplicitLockType(transaction);
            if(lockType_IS_or_IX==LockType.IS && (currLockType==LockType.IS || currLockType==LockType.IX)) {
                return;
            }else if(lockType_IS_or_IX==LockType.IX && currLockType==LockType.IX) {
                return;
            }else if(lockType_IS_or_IX==LockType.IX && currLockType==LockType.SIX) {
                return;
            }else if(lockType_IS_or_IX==LockType.IX && currLockType==LockType.S) {
                ensureSufficientParentLockHeld(lockContext.parentContext(), lockType_IS_or_IX);
                lockContext.promote(transaction, LockType.SIX);
            }
            else {
                ensureSufficientParentLockHeld(lockContext.parentContext(), lockType_IS_or_IX);
                if(lockType_IS_or_IX==LockType.IX && currLockType==LockType.IS) {
                    lockContext.promote(transaction, LockType.IX);
                }else {
                    assert currLockType == LockType.NL;
                    lockContext.acquire(transaction, lockType_IS_or_IX);
                }
            }
        }
    }
}
