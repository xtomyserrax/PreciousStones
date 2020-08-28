package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

abstract class AbstractDBCore {
    
    private RowSetFactory cacheFactory;
    
    AbstractDBCore() {
        
    }
    
    static void setPoolDetails(HikariConfig hikariConf) {
        hikariConf.setMinimumIdle(2);
        hikariConf.setMaximumPoolSize(2);
    }
    
    abstract HikariDataSource getDataSource();
    
    public Connection getConnection() throws SQLException {
        return getDataSource().getConnection();
    }
    
    public void close() {
        getDataSource().close();
    }
    
    private synchronized CachedRowSet createCachedRowSet() throws SQLException {
        if (cacheFactory == null) {
            cacheFactory = RowSetProvider.newFactory();
        }
        return cacheFactory.createCachedRowSet();
    }
    
    private void setArguments(PreparedStatement prepStmt, Object[] args) throws SQLException {
        for (int n = 0; n < args.length; n++) {
            int index = n + 1; // JDBC indexes start at 1
            prepStmt.setObject(index, args[n]);
        }
    }
    
    public ResultSet select(String query, Object...args) throws SQLException {
        try (Connection conn = getConnection(); PreparedStatement prepStmt = conn.prepareStatement(query)) {
            setArguments(prepStmt, args);
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                CachedRowSet cached = createCachedRowSet();
                cached.populate(resultSet);
                return cached;
            }
        }
    }
    
}
