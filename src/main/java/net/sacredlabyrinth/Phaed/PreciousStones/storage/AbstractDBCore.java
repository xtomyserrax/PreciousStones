package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.sacredlabyrinth.Phaed.PreciousStones.PreciousStones;

abstract class AbstractDBCore implements DBCore {
    
    private final VendorType vendorType;
    
    AbstractDBCore(VendorType vendorType) {
        this.vendorType = vendorType;
    }
    
    static void setExtraPoolDetails(HikariConfig hikariConf) {
        hikariConf.setMinimumIdle(2);
        hikariConf.setMaximumPoolSize(2);
    }
    
    abstract HikariDataSource getDataSource();
    
    @Override
    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    
    @Override
    public VendorType getVendorType() {
        return vendorType;
    }
    
    @Override
    public void close() {
        getDataSource().close();
    }
    
    /*
     * Here temporarily
     */
    
    @Override
    public ResultSet select(String query) {
        return null;
    }

    @Override
    public long insert(String query) {
        return 0;
    }

    @Override
    public void update(String query) {
    }

    @Override
    public void delete(String query) {
    }

    @Override
    public Boolean execute(String query) {
        return null;
    }

    @Override
    public Boolean existsTable(String table) {
        try (Connection conn = getConnection();
                ResultSet tables = conn.getMetaData().getTables(null, null, table, null)) {

            return tables.next();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    @Override
    public Boolean existsColumn(String table, String column) {
        try (Connection conn = getConnection();
                ResultSet columns = conn.getMetaData().getColumns(null, null, table, column)) {

            return columns.next();

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    
}
