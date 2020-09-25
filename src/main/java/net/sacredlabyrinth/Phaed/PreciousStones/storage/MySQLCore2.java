package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.sacredlabyrinth.Phaed.PreciousStones.PreciousStones;

class MySQLCore2 extends AbstractDBCore {

    private final HikariDataSource dataSource;
    
    /**
     * Creates from the given credentials
     * 
     * @param host database host
     * @param database database name
     * @param username username
     * @param password password
     */
    MySQLCore2(String host, int port, String database, String username, String password) {
        super(VendorType.MYSQL);
        HikariConfig hikariConf = new HikariConfig();
        hikariConf.setJdbcUrl("jdbc:mysql://" + host + ':' + port + '/' + database + "?useUnicode=true&characterEncoding=utf-8");
        hikariConf.setUsername(username);
        hikariConf.setPassword(password);
        setExtraPoolDetails(hikariConf);
        dataSource = new HikariDataSource(hikariConf);
    }

    @Override
    HikariDataSource getDataSource() {
        return dataSource;
    }
    
    @Override
    public String getDataType(String table, String column) {
        String query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "' AND COLUMN_NAME = '" + column + "';";

        String dataType = "";
        try {
            Statement statement = getConnection().createStatement();

            ResultSet res = statement.executeQuery(query);

            if (res != null) {
                while (res.next()) {
                    dataType = res.getString("DATA_TYPE");
                }
            }
        } catch (Exception ex) {
            //log.severe("Error at SQL Query: " + ex.getMessage());
            //log.severe("Query: " + query);
        }

        PreciousStones.debug("Column %s in table %s has datatype: %s", column, table, dataType);

        if (dataType == null) {
            return "";
        }

        return dataType;
    }
    
}
