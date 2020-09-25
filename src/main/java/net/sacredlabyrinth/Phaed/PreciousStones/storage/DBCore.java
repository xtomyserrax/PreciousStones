package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author phaed
 */
public interface DBCore extends AutoCloseable {
    
    /**
     * Acquires an open connection
     * 
     * @return an open connection, which should be closed when done with
     * @throws SQLException per JDBC
     */
    Connection getConnection() throws SQLException;
    
    /**
     * Gets the vendor used
     * 
     * @return the vendor type
     */
    VendorType getVendorType();

    /**
     * Close connection pool
     */
    @Override
    void close();

    /**
     * Execute a select statement
     *
     * @param query
     * @return
     */
    ResultSet select(String query);

    /**
     * Execute an insert statement
     *
     * @param query
     */
    long insert(String query);

    /**
     * Execute an update statement
     *
     * @param query
     */
    void update(String query);

    /**
     * Execute a delete statement
     *
     * @param query
     */
    void delete(String query);

    /**
     * Execute a statement
     *
     * @param query
     * @return
     */
    Boolean execute(String query);

    /**
     * Check whether a table exists
     *
     * @param table
     * @return
     */
    boolean existsTable(String table);

    /**
     * Check whether a column exists
     *
     * @param table
     * @param column
     * @return
     */
    boolean existsColumn(String table, String column);

    /**
     * CGEt the datatype of a column
     *
     * @param table
     * @param column
     * @return
     */
    String getDataType(String table, String column);
}
