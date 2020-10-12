package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.Connection;
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
     * Check whether a table exists
     *
     * @param conn the connection
     * @param table the table name
     * @return true if the table exists
     */
    boolean tableExists(Connection conn, String table) throws SQLException;

    /**
     * Check whether a column exists
     *
     * @param conn the connection
     * @param table the table name
     * @param column the column name
     * @return true if the column exists
     */
    boolean columnExists(Connection conn, String table, String column) throws SQLException;

    /**
     * If {@link #getDataType(Connection, String, String)} is supported
     * 
     * @return true if supported
     */
    boolean supportsGetDataType();
    
    /**
     * Get the data type of a column
     *
     * @param conn the connection
     * @param table the table name
     * @param column the colum name
     * @return the data type
     * @throws UnsupportedOperationException if not supported by this database implementation
     */
    String getDataType(Connection conn, String table, String column) throws SQLException;

}
