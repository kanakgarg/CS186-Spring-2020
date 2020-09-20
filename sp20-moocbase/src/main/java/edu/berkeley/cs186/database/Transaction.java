package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * The public-facing interface of a transaction.
 */
@SuppressWarnings("unused")
public interface Transaction extends AutoCloseable {
    // Status ///////////////////////////////////////////////////////////////
    enum Status {
        RUNNING,
        COMMITTING,
        ABORTING,
        COMPLETE,
        RECOVERY_ABORTING; // "ABORTING" state for txns during restart recovery

        private static Status[] values = Status.values();

        public int getValue() {
            return ordinal();
        }

        public static Status fromInt(int x) {
            if (x < 0 || x >= values.length) {
                String err = String.format("Unknown TypeId ordinal %d.", x);
                throw new IllegalArgumentException(err);
            }
            return values[x];
        }
    }

    /**
     * @return transaction number
     */
    long getTransNum();

    /**
     * @return current status of transaction
     */
    Status getStatus();

    /**
     * Sets status of transaction. Should not be used directly by
     * users of the transaction (this should be called by the recovery
     * manager).
     * @param status new status of transaction
     */
    void setStatus(Status status);

    /**
     * Commits a transaction. Equivalent to
     *      COMMIT
     *
     * This is the default way a transaction ends.
     */
    void commit();

    /**
     * Rolls back a transaction. Equivalent to
     *      ROLLBACK
     *
     * Project 5 (Recovery) must be fully implemented.
     */
    void rollback();

    /**
     * Cleanup transaction (when transaction ends). Does not
     * need to be called directly, as commit/rollback should
     * call cleanup themselves. Does not do anything on successive calls
     * when called multiple times.
     */
    void cleanup();

    @Override
    void close();

    // DDL //////////////////////////////////////////////////////////////////

    /**
     * Creates a table. Equivalent to
     *      CREATE TABLE tableName (...s)
     *
     * Indices must be created afterwards with createIndex.
     *
     * @param s schema of new table
     * @param tableName name of new table
     */
    void createTable(Schema s, String tableName);

    /**
     * Drops a table. Equivalent to
     *      DROP TABLE tableName
     *
     * @param tableName name of table to drop
     */
    void dropTable(String tableName);

    /**
     * Drops all normal tables.
     */
    void dropAllTables();

    /**
     * Creates an index. Equivalent to
     *      CREATE INDEX tableName_columnName ON tableName (columnName)
     * in postgres.
     *
     * The only index supported is a B+ tree. Indices require Project 2 (B+ trees) to
     * be fully implemented. Bulk loading requires Project 3 Part 1 (Joins/Sorting) to be
     * fully implemented as well.
     *
     * @param tableName name of table to create index for
     * @param columnName name of column to create index on
     * @param bulkLoad whether to bulk load data
     */
    void createIndex(String tableName, String columnName, boolean bulkLoad);

    /**
     * Drops an index. Equivalent to
     *      DROP INDEX tableName_columnName
     * in postgres.
     *
     * @param tableName name of table to drop index from
     * @param columnName name of column to drop index from
     */
    void dropIndex(String tableName, String columnName);

    // DML //////////////////////////////////////////////////////////////////

    /**
     * Returns a QueryPlan selecting from tableName. Equivalent to
     *      SELECT * FROM tableName
     * and used for all SELECT queries.
     * @param tableName name of table to select from
     * @return new query plan
     */
    QueryPlan query(String tableName);

    /**
     * Returns a QueryPlan selecting from tableName. Equivalent to
     *      SELECT * FROM tableName AS alias
     * and used for all SELECT queries.
     * @param tableName name of table to select from
     * @param alias alias of tableName
     * @return new query plan
     */
    QueryPlan query(String tableName, String alias);

    /**
     * Inserts a row into a table. Equivalent to
     *      INSERT INTO tableName VALUES(...values)
     *
     * @param tableName name of table to insert into
     * @param values list of values to insert (in the same order as the table's schema)
     */
    void insert(String tableName, List<DataBox> values);

    /**
     * Updates rows in a table. Equivalent to
     *      UPDATE tableName SET targetColumnName = targetValue(targetColumnName)
     *
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param targetValue function mapping old values to new values
     */
    void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue);

    /**
     * Updates rows in a table. Equivalent to
     *      UPDATE tableName SET targetColumnName = targetValue(targetColumnName)
     *       WHERE predColumnName predOperator predValue
     *
     * @param tableName name of table to update
     * @param targetColumnName column to update
     * @param targetValue function mapping old values to new values
     * @param predColumnName column used in WHERE predicate
     * @param predOperator operator used in WHERE predicate
     * @param predValue value used in WHERE predicate
     */
    void update(String tableName, String targetColumnName, UnaryOperator<DataBox> targetValue,
                String predColumnName, PredicateOperator predOperator, DataBox predValue);

    /**
     * Deletes rows from a table. Equivalent to
     *      DELETE FROM tableNAME WHERE predColumnName predOperator predValue
     *
     * @param tableName name of table to delete from
     * @param predColumnName column used in WHERE predicate
     * @param predOperator operator used in WHERE predicate
     * @param predValue value used in WHERE predicate
     */
    void delete(String tableName, String predColumnName, PredicateOperator predOperator,
                DataBox predValue);

    // Savepoints ///////////////////////////////////////////////////////////

    /**
     * Creates a savepoint. A transaction may roll back to a savepoint it created
     * at any point before committing/aborting. Equivalent to
     *      SAVEPOINT savepointName
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    void savepoint(String savepointName);

    /**
     * Rolls back all changes made by the transaction since the given savepoint.
     * Equivalent to
     *      ROLLBACK TO savepointName
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    void rollbackToSavepoint(String savepointName);

    /**
     * Deletes a savepoint. Equivalent to
     *      RELEASE SAVEPOINT
     *
     * Savepoints require Project 5 (recovery) to be fully implemented.
     *
     * @param savepointName name of savepoint
     */
    void releaseSavepoint(String savepointName);

    // Schema ///////////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    Schema getSchema(String tableName);

    /**
     * Same as getSchema, except all column names are fully qualified (tableName.colName).
     *
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    Schema getFullyQualifiedSchema(String tableName);

    // Statistics ///////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get stats of
     * @return TableStats object of the table
     */
    TableStats getStats(String tableName);

    /**
     * @param tableName name of table
     * @return number of data pages used by the table
     */
    int getNumDataPages(String tableName);

    /**
     * @param tableName name of table
     * @return number of entries that fit on one page for the table
     */
    int getNumEntriesPerPage(String tableName);

    /**
     * @param tableName name of table
     * @return size of a single row for the table
     */
    int getEntrySize(String tableName);

    /**
     * @param tableName name of table
     * @return number of records in the table
     */
    long getNumRecords(String tableName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return order of B+ tree index on tableName.columnName
     */
    int getTreeOrder(String tableName, String columnName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return height of B+ tree index on tableName.columnName
     */
    int getTreeHeight(String tableName, String columnName);

    // Internal /////////////////////////////////////////////////////////////

    /**
     * @return transaction context for this transaction
     */
    TransactionContext getTransactionContext();
}
