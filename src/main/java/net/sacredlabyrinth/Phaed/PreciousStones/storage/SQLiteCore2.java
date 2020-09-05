package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.io.File;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class SQLiteCore2 extends AbstractDBCore {

    private final HikariDataSource dataSource;
    
    /**
     * Creates from file details
     * 
     * @param dbName
     * @param dbLocation
     */
    public SQLiteCore2(String dbName, String dbLocation) {
        super(VendorType.SQLITE);

        File dbFolder = new File(dbLocation);

        if (dbName.contains("/") || dbName.contains("\\") || dbName.endsWith(".db")) {
            throw new IllegalArgumentException("The database name can not contain: /, \\, or .db");
        }
        if (!dbFolder.exists() && !dbFolder.mkdir()) {
            throw new IllegalStateException("Unable to create database folder");
        }
        File file = new File(dbFolder.getAbsolutePath() + File.separator + dbName + ".db");
        HikariConfig hikariConf = new HikariConfig();
        hikariConf.setJdbcUrl("jdbc:sqlite:" + file.getAbsolutePath());
        setExtraPoolDetails(hikariConf);
        dataSource = new HikariDataSource(hikariConf);
    }
    
    @Override
    HikariDataSource getDataSource() {
        return dataSource;
    }
    
    @Override
    public String getDataType(String table, String column) {
        // not supported
        return "";
    }

}
