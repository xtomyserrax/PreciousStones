package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class TableCreator implements AutoCloseable {

    private final DBCore core;
    private final Connection conn;
    private final boolean isMySql;
    
    TableCreator(DBCore core) throws SQLException {
        this.core = core;
        conn = core.getConnection();
        isMySql = core.getVendorType() == VendorType.MYSQL;
    }
    
    void createTables() throws SQLException {
        createCuboids();
        createFields();
        createUnbreakables();
        createGriefUndo();
        createTranslocations();
        createStoredBlocks();
        createPlayers();
    }
    
    private void execute(String createTable) throws SQLException {
        try (PreparedStatement prepStmt = conn.prepareStatement(createTable)) {
            prepStmt.execute();
        }
    }
    
    private void createCuboids() throws SQLException {
        execute(
                "CREATE TABLE IF NOT EXISTS `pstone_cuboids` ("
                + ((isMySql) ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY" : "`id` INTEGER PRIMARY KEY") + ", "

                + "`parent` bigint(20) NOT NULL, "
                + "`x` int(11) default NULL, "
                + "`y` int(11) default NULL, "
                + "`z` int(11) default NULL, "

                + "`world` varchar(25) default NULL, "
                + "`minx` int(11) default NULL, "
                + "`maxx` int(11) default NULL, "
                + "`miny` int(11) default NULL, "
                + "`maxy` int(11) default NULL, "
                + "`minz` int(11) default NULL, "
                + "`maxz` int(11) default NULL, "

                + "`velocity` float default NULL, "
                + "`type_id` int(11) default NULL, "
                + "`data` tinyint default 0, "
                + "`owner` varchar(16) NOT NULL, "
                + "`name` varchar(50) NOT NULL, "
                + "`packed_allowed` text NOT NULL, "
                + "`last_used` bigint(20) Default NULL, "
                + "`flags` TEXT NOT NULL, "
                + ((isMySql) ?
                        "UNIQUE KEY `uq_cuboid_fields_1` (`x`,`y`,`z`,`world`))"
                        : "UNIQUE (`x`,`y`,`z`,`world`))"));
    }
    
    private void createFields() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_fields` ("
                + ((isMySql) ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY" : "`id` INTEGER PRIMARY KEY") + ", "

                + "`x` int(11) default NULL, "
                + "`y` int(11) default NULL, "
                + "`z` int(11) default NULL, "

                + "`world` varchar(25) default NULL, "
                + "`radius` int(11) default NULL, "
                + "`height` int(11) default NULL, "
                + "`velocity` float default NULL, "
                + "`type_id` int(11) default NULL, "

                + "`data` tinyint default 0, "
                + "`owner` varchar(16) NOT NULL, "
                + "`name` varchar(50) NOT NULL, "
                + "`packed_allowed` text NOT NULL, "
                + "`last_used` bigint(20) Default NULL, "
                + "`flags` TEXT NOT NULL, "
                + ((isMySql) ?
                        "UNIQUE KEY `uq_pstone_fields_1` (`x`,`y`,`z`,`world`))"
                        : "UNIQUE (`x`,`y`,`z`,`world`))"));
    }
    
    private void createUnbreakables() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_unbreakables` ("
                + ((isMySql) ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY" : "`id` INTEGER PRIMARY KEY") + ", "
                + "`x` int(11) default NULL, "
                + "`y` int(11) default NULL, "
                + "`z` int(11) default NULL, "
                + "`world` varchar(25) default NULL, "
                + "`owner` varchar(16) NOT NULL, "
                + "`type_id` int(11) default NULL, "
                + "`data` tinyint default 0, "
                + ((isMySql) ?
                        "UNIQUE KEY `uq_pstone_unbreakables_1` (`x`,`y`,`z`,`world`))"
                        : "UNIQUE (`x`,`y`,`z`,`world`))"));
    }
    
    private void createGriefUndo() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_grief_undo` ("
                + ((isMySql) ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY, " : "`id` INTEGER PRIMARY KEY, ")
                + "`date_griefed` bigint(20), "
                + "`field_x` int(11) default NULL, "
                + "`field_y` int(11) default NULL, "
                + "`field_z` int(11) default NULL, "
                + "`world` varchar(25) NOT NULL, "
                + "`x` int(11) default NULL, "
                + "`y` int(11) default NULL, "
                + "`z` int(11) default NULL, "
                + "`type_id` int(11) NOT NULL, "
                + "`data` TINYINT NOT NULL, "
                + "`sign_text` varchar(75) NOT NULL)");
    }
    
    private void createTranslocations() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_translocations` ("
                + (isMySql ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY, " : "`id` INTEGER PRIMARY KEY, ")
                + "`name` varchar(36) NOT NULL, "
                + "`player_name` varchar(16) NOT NULL, "
                + "`minx` int(11) default NULL, "
                + "`maxx` int(11) default NULL, "
                + "`miny` int(11) default NULL, "
                + "`maxy` int(11) default NULL, "
                + "`minz` int(11) default NULL, "
                + "`maxz` int(11) default NULL, "
                + (isMySql ? "UNIQUE KEY `uq_trans_1` (`name`,`player_name`))" : "UNIQUE (`name`,`player_name`))"));
    }
    
    private void createStoredBlocks() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_storedblocks` ("
                + (isMySql ? "`id` bigint(20) NOT NULL auto_increment PRIMARY KEY, " : "`id` INTEGER PRIMARY KEY,  ")
                + "`name` varchar(36) NOT NULL, "
                + "`player_name` varchar(16) NOT NULL, "
                + "`x` int(11) default NULL, "
                + "`y` int(11) default NULL, "
                + "`z` int(11) default NULL, "
                + "`world` varchar(25) NOT NULL, "
                + "`type_id` int(11) NOT NULL, "
                + "`data` TINYINT NOT NULL, "
                + "`sign_text` varchar(75) NOT NULL, "
                + "`applied` bit default 0, "
                + "`contents` TEXT NOT NULL, "
                + (isMySql ? "UNIQUE KEY `uq_trans_2` (`x`,`y`,`z`,`world`))" : "UNIQUE (`x`,`y`,`z`,`world`))"));
    }
    
    private void createPlayers() throws SQLException {
        execute("CREATE TABLE IF NOT EXISTS `pstone_players` ("
                + "`id` bigint(20), "
                + "`uuid` varchar(255) default NULL, "
                + "`player_name` varchar(16) NOT NULL, "
                + "`last_seen` bigint(20) default NULL, "
                + "flags TEXT default NULL, "
                + "PRIMARY KEY (`player_name`))");
    }
    
    @Override
    public void close() throws SQLException {
        conn.close();
    }
    
}
