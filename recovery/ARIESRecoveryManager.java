package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        LogRecord commitLogRecord = new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
        long newLSN = logManager.appendToLog(commitLogRecord);

        transactionTable.get(transNum).lastLSN = newLSN;
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        logManager.flushToLSN(newLSN);

        return newLSN;
        //return -1L;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        LogRecord abortLogRecord = new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
        long newLSN = logManager.appendToLog(abortLogRecord);
        transactionTable.get(transNum).lastLSN = newLSN;
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);

        return newLSN;
        //return -1L;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry xactTableEntry = transactionTable.get(transNum);
        if(xactTableEntry.transaction.getStatus() == Transaction.Status.ABORTING) {
            LogRecord currLogRecord = logManager.fetchLogRecord(xactTableEntry.lastLSN);
            while(true) {
                if(currLogRecord.getLSN() == 0) {
                    break;
                }
                if(currLogRecord.isUndoable()) {
                    Pair<LogRecord, Boolean> p = currLogRecord.undo(xactTableEntry.lastLSN);
                    //Pair<LogRecord, Boolean> p = currLogRecord.undo(currLogRecord.getLSN());
                    LogRecord newCLRlogRecord = p.getFirst();
                    boolean need_to_flush = p.getSecond();
                    long newLSN = logManager.appendToLog(newCLRlogRecord);
                    transactionTable.get(transNum).lastLSN = newLSN;
                    if(need_to_flush) {
                        logManager.flushToLSN(newLSN);
                    }
                    newCLRlogRecord.redo(diskSpaceManager, bufferManager);

                    // update DPT as necessary
                    LogType logType = newCLRlogRecord.getType();
                    if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE) {
                        long pageNum = newCLRlogRecord.getPageNum().get();
                        if(!dirtyPageTable.containsKey(pageNum)) {
                            dirtyPageTable.put(pageNum, newCLRlogRecord.getLSN());
                        }
                    }else if(logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
                            || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE) {
                        long pageNum = newCLRlogRecord.getPageNum().get();
                        if(dirtyPageTable.containsKey(pageNum)) {
                            dirtyPageTable.remove(pageNum);
                        }
                    }
                }
                if(currLogRecord.getUndoNextLSN().isPresent()) {
                    currLogRecord = logManager.fetchLogRecord(currLogRecord.getUndoNextLSN().get());
                }else {
                    if(currLogRecord.getPrevLSN().isPresent()) {
                        currLogRecord = logManager.fetchLogRecord(currLogRecord.getPrevLSN().get());
                    }else {
                        break;
                    }
                }
            }
        }else {
            assert xactTableEntry.transaction.getStatus() == Transaction.Status.COMMITTING;
        }
        LogRecord endLogRecord = new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
        long endLSN = logManager.appendToLog(endLogRecord);
        transactionTable.get(transNum).transaction.cleanup();
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);

        return endLSN;
        //return -1L;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5): implement
        if(after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            long prevLSN1 = transactionTable.get(transNum).lastLSN;
            LogRecord undoLogRec = new UpdatePageLogRecord(transNum, pageNum, prevLSN1, pageOffset, before, null);
            long undoLSN = logManager.appendToLog(undoLogRec);
            transactionTable.get(transNum).lastLSN = undoLSN;
            if(!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, undoLSN);
            }

            long prevLSN2 = transactionTable.get(transNum).lastLSN;
            LogRecord redoLogRec = new UpdatePageLogRecord(transNum, pageNum, prevLSN2, pageOffset, null, after);
            long redoLSN = logManager.appendToLog(redoLogRec);
            transactionTable.get(transNum).lastLSN = redoLSN;
        }else {
            long prevLSN = transactionTable.get(transNum).lastLSN;
            LogRecord updateLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            long newLSN = logManager.appendToLog(updateLogRecord);
            transactionTable.get(transNum).lastLSN = newLSN;
            if(!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, newLSN);
            }
        }
        transactionTable.get(transNum).touchedPages.add(pageNum);
        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        LogRecord currLogRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        while(true) {
            if(currLogRecord.getLSN() <= LSN) {
                break;
            }
            if(currLogRecord.isUndoable()) {
                Pair<LogRecord, Boolean> p = currLogRecord.undo(transactionEntry.lastLSN);
                //Pair<LogRecord, Boolean> p = currLogRecord.undo(currLogRecord.getLSN());
                LogRecord newCLRlogRecord = p.getFirst();
                boolean need_to_flush = p.getSecond();
                long newLSN = logManager.appendToLog(newCLRlogRecord);
                transactionTable.get(transNum).lastLSN = newLSN;
                if(need_to_flush) {
                    logManager.flushToLSN(newLSN);

                }
                newCLRlogRecord.redo(diskSpaceManager, bufferManager);

            }
            if(currLogRecord.getUndoNextLSN().isPresent()) {
                if(currLogRecord.getUndoNextLSN().get() == LSN) {
                    break;
                }
                currLogRecord = logManager.fetchLogRecord(currLogRecord.getUndoNextLSN().get());
            }else {
                if(currLogRecord.getPrevLSN().isPresent()) {
                    if(currLogRecord.getPrevLSN().get() == LSN) {
                        break;
                    }
                    currLogRecord = logManager.fetchLogRecord(currLogRecord.getPrevLSN().get());
                }else {
                    break;
                }
            }
        }
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            boolean fitsAfterAdd;
            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size()+1, txnTable.size(), touchedPages.size(), numTouchedPages);
            if(!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            dpt.put(entry.getKey(), entry.getValue());
        }

        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            boolean fitsAfterAdd;
            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                dpt.size(), txnTable.size()+1, touchedPages.size(), numTouchedPages);
            if(!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            txnTable.put(transNum, new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        restartAnalysis();
        restartRedo();

        Set<Long> dirtyPageNums = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if(dirty) {
                dirtyPageNums.add(pageNum);
            }
        });
        Set<Long> nonDirtyPageNums = new HashSet<>();
        for(Long pageNum : dirtyPageTable.keySet()) {
            if(!dirtyPageNums.contains(pageNum)) {
                nonDirtyPageNums.add(pageNum);
            }
        }
        for(Long nonDirtyPageNum : nonDirtyPageNums) {
            dirtyPageTable.remove(nonDirtyPageNum);
        }

        return () -> {
            restartUndo();
            checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        Iterator<LogRecord> logRecordIter = logManager.scanFrom(LSN);
        while(logRecordIter.hasNext()) {
            LogRecord currLogRecord = logRecordIter.next();
            LogType logType = currLogRecord.getType();
            if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE
            || logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
            || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE
            || logType==LogType.ALLOC_PART || logType==LogType.UNDO_ALLOC_PART
            || logType==LogType.FREE_PART || logType==LogType.UNDO_FREE_PART) {
                long transNum = currLogRecord.getTransNum().get();
                if(!transactionTable.containsKey(transNum)) {
                    transactionTable.put(transNum, new TransactionTableEntry(newTransaction.apply(transNum)));
                }
                transactionTable.get(transNum).lastLSN = currLogRecord.getLSN();
                if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE
                || logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
                || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE) {
                    long pageNum = currLogRecord.getPageNum().get();
                    transactionTable.get(transNum).touchedPages.add(pageNum);
                    Transaction transaction = transactionTable.get(transNum).transaction;
                    acquireTransactionLock(transaction, getPageLockContext(pageNum), LockType.X);
                    if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE) {
                        if(!dirtyPageTable.containsKey(pageNum)) {
                            dirtyPageTable.put(pageNum, currLogRecord.getLSN());
                        }
                    }else if(logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
                            || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE) {
                        if(dirtyPageTable.containsKey(pageNum)) {
                            dirtyPageTable.remove(pageNum);
                        }
                    }
                }
            }else if(logType==LogType.COMMIT_TRANSACTION || logType==LogType.ABORT_TRANSACTION
            || logType==LogType.END_TRANSACTION) {
                long transNum = currLogRecord.getTransNum().get();
                if(!transactionTable.containsKey(transNum)) {
                    transactionTable.put(transNum, new TransactionTableEntry(newTransaction.apply(transNum)));
                }
                if(logType == LogType.END_TRANSACTION) {
                    transactionTable.get(transNum).transaction.cleanup();
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                }else {
                    transactionTable.get(transNum).lastLSN = currLogRecord.getLSN();
                    if(logType == LogType.COMMIT_TRANSACTION) {
                        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
                    }else {
                        assert logType == LogType.ABORT_TRANSACTION;
                        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    }

                }
            }else if(logType==LogType.BEGIN_CHECKPOINT) {
                updateTransactionCounter.accept(Long.max(
                        getTransactionCounter.get(), currLogRecord.getMaxTransactionNum().get()));
            }else {
                assert logType==LogType.END_CHECKPOINT;
                for(Map.Entry<Long, Long> entry : currLogRecord.getDirtyPageTable().entrySet()) {
                    Long pageNum = entry.getKey();
                    Long recLSN = entry.getValue();
                    if(!dirtyPageTable.containsKey(pageNum)) {
                        dirtyPageTable.put(pageNum, recLSN);
                    }else {
                        dirtyPageTable.replace(pageNum, recLSN);
                    }
                }
                for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : currLogRecord.getTransactionTable().entrySet()) {
                    Long transNum = entry.getKey();
                    Pair<Transaction.Status, Long> p = entry.getValue();
                    Transaction.Status status = p.getFirst();
                    Long lastLSN = p.getSecond();
                    if(!transactionTable.containsKey(transNum)) {
                        transactionTable.put(transNum, new TransactionTableEntry(newTransaction.apply(transNum)));
                        // special case: the status.complete record comes back
                        if(transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.COMPLETE) {
                            transactionTable.remove(transNum);
                        }else {
                            transactionTable.get(transNum).transaction.setStatus(status);
                            transactionTable.get(transNum).lastLSN = lastLSN;
                        }
                    }else {
                        // update status
                        if(status != transactionTable.get(transNum).transaction.getStatus()) {
                            if(status == Transaction.Status.RUNNING) {
                                // transactionTable keeps its own status
                            }else if(transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.RUNNING) {
                                transactionTable.get(transNum).transaction.setStatus(status);
                            }
                        }
                        // special case for abort to recovery abort status
                        if(transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING) {
                            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        }

                        // update lastLSN
                        if(lastLSN > transactionTable.get(transNum).lastLSN) {
                            // do we need to change status?
                            //transactionTable.get(transNum).transaction.setStatus(status);  // for debug purpose
                            transactionTable.get(transNum).lastLSN = lastLSN;
                        }
                    }
                }
                for(Map.Entry<Long, List<Long>> entry : currLogRecord.getTransactionTouchedPages().entrySet()) {
                    Long transNum = entry.getKey();
                    List<Long> touchedPages = entry.getValue();
                    assert transactionTable.containsKey(transNum);
                    Transaction transaction = transactionTable.get(transNum).transaction;
                    if(transactionTable.get(transNum).transaction.getStatus() != Transaction.Status.COMPLETE) {
                        for(Long touchedPage : touchedPages) {
                            if(!transactionTable.get(transNum).touchedPages.contains(touchedPage)) {
                                transactionTable.get(transNum).touchedPages.add(touchedPage);
                                acquireTransactionLock(transaction, getPageLockContext(touchedPage), LockType.X);
                            }
                        }
                    }
                }
            }
        }
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            Long transNum = entry.getKey();
            TransactionTableEntry xactTableEntry = entry.getValue();
            if(xactTableEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                xactTableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                LogRecord abortLogRecord = new AbortTransactionLogRecord(transNum, xactTableEntry.lastLSN);
                long newAbortLSN = logManager.appendToLog(abortLogRecord);
                xactTableEntry.lastLSN = newAbortLSN;
            }else if(xactTableEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                xactTableEntry.transaction.cleanup();
                xactTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                LogRecord endLogRecord = new EndTransactionLogRecord(transNum, xactTableEntry.lastLSN);
                logManager.appendToLog(endLogRecord);
                transactionTable.remove(transNum);
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5): implement
        long minLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> logRecordIter = logManager.scanFrom(minLSN);
        while(logRecordIter.hasNext()) {
            LogRecord currLogRecord = logRecordIter.next();
            LogType logType = currLogRecord.getType();
            long LSN = currLogRecord.getLSN();
            if(currLogRecord.isRedoable()) {
                if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE
                        || logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
                        || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE) {
                    long pageNum = currLogRecord.getPageNum().get();
                    if(dirtyPageTable.containsKey(pageNum) && LSN>=dirtyPageTable.get(pageNum)) {
                        Page page = bufferManager.fetchPage(getPageLockContext(pageNum).parentContext(), pageNum, false);
                        try {
                            if(page.getPageLSN() < LSN) {
                                currLogRecord.redo(diskSpaceManager, bufferManager);
                            }
                        } finally {
                            page.unpin();
                        }
                    }
                }else if(logType==LogType.ALLOC_PART || logType==LogType.UNDO_ALLOC_PART
                        || logType==LogType.FREE_PART || logType==LogType.UNDO_FREE_PART) {
                    currLogRecord.redo(diskSpaceManager, bufferManager);
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        //Pair<Long lastLSN, Long transNum>
        PriorityQueue<Pair<Long, Long>> toUndo = new PriorityQueue<>(new PairFirstReverseComparator());
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry xactTableEntry = entry.getValue();
            if(xactTableEntry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                toUndo.add(new Pair<>(xactTableEntry.lastLSN, transNum));
            }
        }
        while(!toUndo.isEmpty()) {
            Pair<Long, Long> p = toUndo.poll();
            long currLSN = p.getFirst();
            long transNum = p.getSecond();
            LogRecord currLogRecord = logManager.fetchLogRecord(currLSN);

            if(currLogRecord.isUndoable()) {
                Pair<LogRecord, Boolean> p2 = currLogRecord.undo(transactionTable.get(transNum).lastLSN);
                //Pair<LogRecord, Boolean> p2 = currLogRecord.undo(currLogRecord.getLSN());
                LogRecord newCLRlogRecord = p2.getFirst();
                boolean need_to_flush = p2.getSecond();
                long newLSN = logManager.appendToLog(newCLRlogRecord);
                transactionTable.get(transNum).lastLSN = newLSN;
                if(need_to_flush) {
                    logManager.flushToLSN(newLSN);
                }
                newCLRlogRecord.redo(diskSpaceManager, bufferManager);

                // update DPT as necessary
                LogType logType = newCLRlogRecord.getType();
                if(logType==LogType.UPDATE_PAGE || logType==LogType.UNDO_UPDATE_PAGE) {
                    long pageNum = newCLRlogRecord.getPageNum().get();
                    if(!dirtyPageTable.containsKey(pageNum)) {
                        dirtyPageTable.put(pageNum, newCLRlogRecord.getLSN());
                    }
                }else if(logType==LogType.ALLOC_PAGE || logType==LogType.UNDO_ALLOC_PAGE
                        || logType==LogType.FREE_PAGE || logType==LogType.UNDO_FREE_PAGE) {
                    long pageNum = newCLRlogRecord.getPageNum().get();
                    if(dirtyPageTable.containsKey(pageNum)) {
                        dirtyPageTable.remove(pageNum);
                    }
                }
            }
            if(currLogRecord.getUndoNextLSN().isPresent()) {
                toUndo.add(new Pair<>(currLogRecord.getUndoNextLSN().get(), transNum));
            }else {
                if(currLogRecord.getPrevLSN().get() != 0) {
                    toUndo.add(new Pair<>(currLogRecord.getPrevLSN().get(), transNum));
                }else {
                    LogRecord endLogRecord = new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
                    logManager.appendToLog(endLogRecord);
                    transactionTable.get(transNum).transaction.cleanup();
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(transNum);
                }

            }
        }

    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
