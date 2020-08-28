package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MySQLCore2 extends AbstractDBCore {

    private final HikariDataSource dataSource;
    
    /**
     * Creates from the given credentials
     * 
     * @param host database host
     * @param database database name
     * @param username username
     * @param password password
     */
    public MySQLCore2(String host, int port, String database, String username, String password) {
        HikariConfig hikariConf = new HikariConfig();
        hikariConf.setJdbcUrl("jdbc:mysql://" + host + ':' + port + '/' + database + "?useUnicode=true&characterEncoding=utf-8");
        hikariConf.setUsername(username);
        hikariConf.setPassword(password);
        setPoolDetails(hikariConf);
        dataSource = new HikariDataSource(hikariConf);
    }

    @Override
    HikariDataSource getDataSource() {
        return dataSource;
    }
    
}
