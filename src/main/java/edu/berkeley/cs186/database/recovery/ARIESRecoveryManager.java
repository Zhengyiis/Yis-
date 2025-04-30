package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {

    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

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
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // Get the transaction table entry
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        // Get the previous LSN of this transaction
        long preLSN = transactionEntry.lastLSN;
        // Create a commit log record
        LogRecord commitRecord = new CommitTransactionLogRecord(transNum, preLSN);
        // Append the record to the log and get its LSN
        long comLSN = logManager.appendToLog(commitRecord);
        // Update the transaction's lastLSN
        transactionEntry.lastLSN = comLSN;
        // Update the transaction status
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        // Flush the log to ensure durability before returning
        logManager.flushToLSN(comLSN);
        return comLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long preLSN = transactionEntry.lastLSN;
        long aboLSN= logManager.appendToLog(new AbortTransactionLogRecord(transNum, preLSN));
        transactionEntry.lastLSN = aboLSN;
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        return aboLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        // Get the transaction table entry for this transaction
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // Check if the transaction is in ABORTING state
        if (transactionTableEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            // Get the most recent log record for this transaction
            LogRecord logR = logManager.fetchLogRecord(transactionTableEntry.lastLSN);

            // Go all the way back to the first log record of the transaction
            while (logR != null && logR.getPrevLSN().isPresent()) {
                Long preLSN = logR.getPrevLSN().get();
                logR = logManager.fetchLogRecord(preLSN);
            }

            // Rollback the transaction to the very beginning
            rollbackToLSN(transNum, logR.getLSN());
        }

        // Get the current lastLSN (which may have been updated during rollback)
        long lastLSN = transactionTableEntry.lastLSN;

        // Create an end transaction log record
        EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transNum, lastLSN);

        // Append the end record to the log
        long endLSN = logManager.appendToLog(endRecord);

        // Update the transaction's lastLSN
        transactionTableEntry.lastLSN = endLSN;

        // Update the transaction status to COMPLETE
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);

        // Remove the transaction from the transaction table
        transactionTable.remove(transNum);

        // Return the LSN of the end record
        return endLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        // Get the transaction entry
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);

        // Get the most recent log record
        LogRecord lastR = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRLSN = lastR.getLSN();

        // Determine where to start undoing from
        long currentLSN = lastR.getUndoNextLSN().orElse(lastRLSN);

        // Continue undoing until we reach the target LSN
        while (currentLSN > LSN) {
            // Get the record at the current LSN
            LogRecord logR = logManager.fetchLogRecord(currentLSN);

            // Check if the record is undoable
            if (logR.isUndoable()) {
                // Get CLR (Compensation Log Record)
                LogRecord clr = logR.undo(transactionEntry.lastLSN);

                // Append the CLR to the log
                long lsn = logManager.appendToLog(clr);

                // Update the transaction's lastLSN
                transactionEntry.lastLSN = lsn;

                // Call redo on the CLR to perform the undo operation
                clr.redo(this, diskSpaceManager, bufferManager);
            }

            // Move to the previous record
            currentLSN = logR.getPrevLSN().orElse(-1L);
        }
        // TODO(proj5) implement the rollback logic described above
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
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
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
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
        long lastLSN = transactionTableEntry.lastLSN;
        UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);
        long lsn = logManager.appendToLog(updatePageLogRecord);
        transactionTableEntry.lastLSN = lsn;
        dirtyPageTable.putIfAbsent(pageNum, lsn);

        return lsn;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
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
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

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
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
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
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
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
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum,savepointLSN);
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        //Temporary maps chkptDPT and chkptTxnTable store the snapshot of the dirty page table and transaction table, respectively.
        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        int pageN = 0;
        int transactionN = 0;
        //iterates over all entries in the dirtyPageTable
        for (Long pageNum : dirtyPageTable.keySet()) {
            Long recLSN = dirtyPageTable.get(pageNum);
            // If adding another page exceeds the space:new checkpoint
            if (!EndCheckpointLogRecord.fitsInOneRecord(pageN + 1, transactionN)) {
                EndCheckpointLogRecord endpoint = new EndCheckpointLogRecord(new HashMap<>(chkptDPT), new HashMap<>(chkptTxnTable));
                logManager.appendToLog(endpoint);
                flushToLSN(endpoint.getLSN());
                chkptDPT.clear();
                pageN = 0;
            }
            pageN += 1;
            chkptDPT.put(pageNum, recLSN);
        }

        for (Long transNum : transactionTable.keySet()) {
            TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);
            if (!EndCheckpointLogRecord.fitsInOneRecord(pageN, transactionN + 1)) {
                EndCheckpointLogRecord endpoint = new EndCheckpointLogRecord(new HashMap<>(chkptDPT), new HashMap<>(chkptTxnTable));
                logManager.appendToLog(endpoint);
                flushToLSN(endpoint.getLSN());
                chkptDPT.clear();
                chkptTxnTable.clear();
                pageN = 0;
                transactionN = 0;
            }
            Transaction transaction = transactionTableEntry.transaction;
            chkptTxnTable.put(transNum, new Pair<>(transaction.getStatus(), transactionTableEntry.lastLSN));
            transactionN += 1;
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> iterator = logManager.scanFrom(LSN);
        while (iterator.hasNext()) {
            LogRecord logR = iterator.next();
            //  If the log record is for a transaction operation (getTransNum is present)
            if (logR.getTransNum().isPresent()) {
                Long transN = logR.getTransNum().get();
                // update the transaction table
                if (transactionTable.get(transN) == null) {
                    Transaction t = newTransaction.apply(transN);
                    startTransaction(t);
                }
                // update LSN
                TransactionTableEntry tableEntry = transactionTable.get(transN);
                tableEntry.lastLSN = logR.getLSN();
            }
            // If the log record is page-related (getPageNum is present), update the dpt
            if (logR.getPageNum().isPresent()) {
                LogType ltype = logR.getType();
                // update/undoupdate page will dirty pages
                if (ltype.equals(LogType.UPDATE_PAGE) || ltype.equals(LogType.UNDO_UPDATE_PAGE)) {
                    dirtyPage(logR.getPageNum().get(), logR.getLSN());
                }
                // free/undoalloc page always flush changes to disk
                if (ltype.equals(LogType.FREE_PAGE) || ltype.equals(LogType.UNDO_ALLOC_PAGE)) {
                    pageFlushHook(logR.getLSN());
                    dirtyPageTable.remove(logR.getPageNum().get());
                }
                //  no action needed for alloc/undofree page
            }

            // If the log record is for a change in transaction status:
            //update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE

            if (logR.getType().equals(LogType.COMMIT_TRANSACTION)) {
                Long tn = logR.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(tn);
                tableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
            }

            if (logR.getType().equals(LogType.ABORT_TRANSACTION)) {
                Long tn = logR.getTransNum().get();
                TransactionTableEntry tableEntry = transactionTable.get(tn);
                tableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }

            // if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
            //   from txn table, and add to endedTransactions
            if (logR.getType().equals(LogType.END_TRANSACTION)) {
                Long tn = logR.getTransNum().get();
                TransactionTableEntry ttableEntry = transactionTable.get(tn);
                ttableEntry.transaction.cleanup();
                transactionTable.remove(tn);
                endedTransactions.add(tn);
                ttableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            }

            //If the log record is an end_checkpoint record:
            if (logR.getType().equals(LogType.END_CHECKPOINT)) {
                // Copy all entries of checkpoint DPT (replace existing entries if any)
                Map<Long, Long> dpt = logR.getDirtyPageTable();
                dirtyPageTable.putAll(dpt);
                Map<Long, Pair<Transaction.Status, Long>> trTable = logR.getTransactionTable();

                for (Long transN : trTable.keySet()) {
                    //Skip txn table entries for transactions that have already ended
                    if (endedTransactions.contains(transN)) continue;
                    if (!transactionTable.containsKey(transN)) {
                        startTransaction(newTransaction.apply(transN));
                    }
                    Pair<Transaction.Status, Long> pair = trTable.get(transN);
                    TransactionTableEntry te = transactionTable.get(transN);
                    Long lsn = pair.getSecond();
                    if (lsn >= te.lastLSN) {
                        te.lastLSN = lsn;
                    }

                    if (IfAdvance(pair.getFirst(), te.transaction.getStatus())) {
                        if (pair.getFirst().equals(Transaction.Status.ABORTING)) {
                            te.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        } else {
                            te.transaction.setStatus(pair.getFirst());
                        }
                    }
                }
            }


        }
        for (Long trn : transactionTable.keySet()) {
            TransactionTableEntry ttableEntry = transactionTable.get(trn);
            Transaction tra = ttableEntry.transaction;

            if (tra.getStatus().equals(Transaction.Status.COMMITTING)) {
                tra.cleanup();
                end(trn);
            }

            if (tra.getStatus().equals(Transaction.Status.RUNNING)) {
                abort(trn);
                tra.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }

        }
        return;
    }

    private boolean IfAdvance(Transaction.Status t1, Transaction.Status t2) {
        if (t1.equals(Transaction.Status.RUNNING)) return false;
        if (t1.equals(Transaction.Status.ABORTING) || t1.equals(Transaction.Status.COMMITTING)) {
            if (t2.equals(Transaction.Status.RUNNING)) return true;
            else return false;
        }
        if (t1.equals(Transaction.Status.COMPLETE)) return true;
        return true;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement
        long lowRecLSN = get_LowRecLSN();
        Iterator<LogRecord> iterator = logManager.scanFrom(lowRecLSN);
        while (iterator.hasNext()) {
            LogRecord logR = iterator.next();
            LogType tt = logR.getType();
            if (tt.equals(LogType.ALLOC_PART)
                    || tt.equals(LogType.FREE_PART)
                    || tt.equals(LogType.UNDO_FREE_PART)
                    || tt.equals(LogType.UNDO_ALLOC_PART)) {
                logR.redo(this, diskSpaceManager, bufferManager);
            }
            if (tt.equals(LogType.ALLOC_PAGE)
                    || tt.equals(LogType.UNDO_FREE_PAGE)) {
                logR.redo(this, diskSpaceManager, bufferManager);
            }
            if (tt.equals(LogType.UPDATE_PAGE)
                    || tt.equals(LogType.UNDO_UPDATE_PAGE)
                    || tt.equals(LogType.FREE_PAGE)
                    || tt.equals(LogType.UNDO_ALLOC_PAGE)) {
                Long pageNum = logR.getPageNum().get();

                if (dirtyPageTable.containsKey(pageNum)
                        && logR.getLSN() >= dirtyPageTable.get(pageNum)) {
                    Page pp = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                    try {
                        if (pp.getPageLSN() < logR.getLSN()) {
                            logR.redo(this, diskSpaceManager, bufferManager);
                        }
                    } finally {
                        pp.unpin();
                    }
                }
            }
        }
        return;
    }

    /** get lowest recLSN from dpt*/
    private long get_LowRecLSN() {
        return dirtyPageTable.values().stream()
                .sorted()
                .limit(1)
                .collect(Collectors.toList())
                .get(0);
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Pair<Long, Long>> PriorityQueue = new PriorityQueue<>((a, b) -> (int)(b.getSecond() - a.getSecond()));
        PriorityQueue.addAll(get_AbortTransactionLastLSN());
        while (!PriorityQueue.isEmpty()) {
            Pair<Long, Long> pair = PriorityQueue.poll();
            Long lastLSN = pair.getSecond();
            Long trn = pair.getFirst();
            LogRecord logR = logManager.fetchLogRecord(lastLSN);
            TransactionTableEntry TransactionTableEntry = transactionTable.get(trn);
            if (logR.isUndoable()) {
                LogRecord CLR = logR.undo(TransactionTableEntry.lastLSN);
                long lsn = logManager.appendToLog(CLR);
                TransactionTableEntry.lastLSN = lsn;
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            long newL;
            if (logR.getUndoNextLSN().isPresent()) {
                newL = logR.getUndoNextLSN().get();
            } else {
                newL = logR.getPrevLSN().orElse(0L);
            }
            if (newL == 0) {
                TransactionTableEntry.transaction.cleanup();
                end(trn);
                continue;
            }
            PriorityQueue.add(new Pair<>(trn, newL));
        }

        return;
    }

    private List<Pair<Long, Long>> get_AbortTransactionLastLSN() {
        List<Pair<Long, Long>> rett = new ArrayList<>();
        for (Long t : transactionTable.keySet()) {
            Transaction tt = transactionTable.get(t).transaction;
            if (tt.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                rett.add(new Pair<>(t, transactionTable.get(t).lastLSN));
            }
        }
        return rett;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
