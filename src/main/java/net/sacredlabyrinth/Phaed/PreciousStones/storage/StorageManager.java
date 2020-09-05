package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import net.sacredlabyrinth.Phaed.PreciousStones.DirtyFieldReason;
import net.sacredlabyrinth.Phaed.PreciousStones.PreciousStones;
import net.sacredlabyrinth.Phaed.PreciousStones.blocks.GriefBlock;
import net.sacredlabyrinth.Phaed.PreciousStones.blocks.TranslocationBlock;
import net.sacredlabyrinth.Phaed.PreciousStones.blocks.Unbreakable;
import net.sacredlabyrinth.Phaed.PreciousStones.entries.BlockTypeEntry;
import net.sacredlabyrinth.Phaed.PreciousStones.entries.PlayerEntry;
import net.sacredlabyrinth.Phaed.PreciousStones.entries.PurchaseEntry;
import net.sacredlabyrinth.Phaed.PreciousStones.entries.SnitchEntry;
import net.sacredlabyrinth.Phaed.PreciousStones.field.Field;
import net.sacredlabyrinth.Phaed.PreciousStones.field.FieldFlag;
import net.sacredlabyrinth.Phaed.PreciousStones.field.FieldSettings;
import net.sacredlabyrinth.Phaed.PreciousStones.helpers.ChatHelper;
import net.sacredlabyrinth.Phaed.PreciousStones.helpers.Helper;
import net.sacredlabyrinth.Phaed.PreciousStones.uuid.UUIDMigration;
import net.sacredlabyrinth.Phaed.PreciousStones.vectors.Vec;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitTask;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.time.temporal.ChronoUnit.DAYS;

/**
 * @author phaed
 */
public class StorageManager {
    /**
     *
     */
    private DBCore core;
    private PreciousStones plugin;
    private final Map<Vec, Field> pending = new HashMap<>();
    private final Set<Field> pendingGrief = new HashSet<>();
    private final Map<Unbreakable, Boolean> pendingUb = new HashMap<>();
    private final Map<String, Boolean> pendingPlayers = new HashMap<>();
    private final List<SnitchEntry> pendingSnitchEntries = new ArrayList<>();
    private boolean haltUpdates;

    /**
     *
     */
    public StorageManager() {
        plugin = PreciousStones.getInstance();

        initiateDB();
        loadWorldData();
        saverScheduler();
        purgePlayers();
    }

    private void initiateDB() {
        if (plugin.getSettingsManager().isUseMysql()) {
            core = new MySQLCore(plugin.getSettingsManager().getHost(), plugin.getSettingsManager().getPort(), plugin.getSettingsManager().getDatabase(), plugin.getSettingsManager().getUsername(), plugin.getSettingsManager().getPassword());

            PreciousStones.log("dbMysqlConnected");

            if (!core.existsTable("pstone_cuboids")) {
                PreciousStones.log("Creating table: pstone_cuboids");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_cuboids` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
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
                        + "PRIMARY KEY (`id`), "
                        + "UNIQUE KEY `uq_cuboid_fields_1` (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_fields")) {
                PreciousStones.log("Creating table: pstone_fields");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_fields` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
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
                        + "PRIMARY KEY (`id`), "
                        + "UNIQUE KEY `uq_pstone_fields_1` (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_unbreakables")) {
                PreciousStones.log("Creating table: pstone_unbreakables");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_unbreakables` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
                        + "`x` int(11) default NULL, "
                        + "`y` int(11) default NULL, "
                        + "`z` int(11) default NULL, "
                        + "`world` varchar(25) default NULL, "
                        + "`owner` varchar(16) NOT NULL, "
                        + "`type_id` int(11) default NULL, "
                        + "`data` tinyint default 0, "
                        + "PRIMARY KEY (`id`), "
                        + "UNIQUE KEY `uq_pstone_unbreakables_1` (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_grief_undo")) {
                PreciousStones.log("Creating table: pstone_grief_undo");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_grief_undo` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
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
                        + "`sign_text` varchar(75) NOT NULL, "
                        + "PRIMARY KEY  (`id`))");
            }

            if (!core.existsTable("pstone_translocations")) {
                PreciousStones.log("Creating table: pstone_translocations");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_translocations` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
                        + "`name` varchar(36) NOT NULL, "
                        + "`player_name` varchar(16) NOT NULL, "
                        + "`minx` int(11) default NULL, "
                        + "`maxx` int(11) default NULL, "
                        + "`miny` int(11) default NULL, "
                        + "`maxy` int(11) default NULL, "
                        + "`minz` int(11) default NULL, "
                        + "`maxz` int(11) default NULL, "
                        + "PRIMARY KEY (`id`), "
                        + "UNIQUE KEY `uq_trans_1` (`name`,`player_name`))");
            }

            if (!core.existsTable("pstone_storedblocks")) {
                PreciousStones.log("Creating table: pstone_storedblocks");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_storedblocks` ("
                        + "`id` bigint(20) NOT NULL auto_increment, "
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
                        + "PRIMARY KEY  (`id`), "
                        + "UNIQUE KEY `uq_trans_2` (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_players")) {
                PreciousStones.log("Creating table: pstone_players");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_players` ("
                        + "`id` bigint(20), "
                        + "`uuid` varchar(255) default NULL, "
                        + "`player_name` varchar(16) NOT NULL, "
                        + "`last_seen` bigint(20) default NULL, "
                        + "flags TEXT default NULL, "
                        + "PRIMARY KEY (`player_name`))");
            }

            if (!core.existsTable("pstone_snitches")) {
                PreciousStones.log("Creating table: pstone_snitches");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_snitches` ("
                        + "`id` bigint(20), `x` int(11) default NULL, "
                        + "`y` int(11) default NULL, "
                        + "`z` int(11) default NULL, "
                        + "`world` varchar(25) default NULL, "
                        + "`name` varchar(16) NOT NULL, "
                        + "`reason` varchar(20) default NULL, "
                        + "`details` varchar(50) default NULL, "
                        + "`count` int(11) default NULL, "
                        + "`date` varchar(25) default NULL, "
                        + "PRIMARY KEY  (`x`, `y`, `z`, `world`, `name`, `reason`, `details`))");

                addIndexes();
            }

            if (!core.existsTable("pstone_purchase_payments")) {
                PreciousStones.log("Creating table: pstone_purchase_payments");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_purchase_payments` ("
                        + "`id` bigint(20), "
                        + "`buyer` varchar(16) default NULL, "
                        + "`owner` varchar(16) NOT NULL, "
                        + "`item` varchar(20) default NULL, "
                        + "`amount` int(11) default NULL, "
                        + "`fieldName` varchar(255) default NULL, "
                        + "`coords` varchar(255) default NULL)");

                addIndexes();
            }
        } else {
            core = new SQLiteCore("PreciousStones", plugin.getDataFolder().getPath());

            PreciousStones.log("dbSqliteConnected");

            if (!core.existsTable("pstone_cuboids")) {
                PreciousStones.log("Creating table: pstone_cuboids");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_cuboids` ("
                        + "`id` INTEGER PRIMARY KEY, "
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
                        + "`flags` TEXT NOT NULL, UNIQUE (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_fields")) {
                PreciousStones.log("Creating table: pstone_fields");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_fields` ("
                        + "`id` INTEGER PRIMARY KEY, "
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
                        + "UNIQUE (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_unbreakables")) {
                PreciousStones.log("Creating table: pstone_unbreakables");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_unbreakables` ("
                        + "`id` INTEGER PRIMARY KEY, "
                        + "`x` int(11) default NULL, "
                        + "`y` int(11) default NULL, "
                        + "`z` int(11) default NULL, "
                        + "`world` varchar(25) default NULL, "
                        + "`owner` varchar(16) NOT NULL, "
                        + "`type_id` int(11) default NULL, "
                        + "`data` tinyint default 0, "
                        + "UNIQUE (`x`,`y`,`z`,`world`))");
            }

            if (!core.existsTable("pstone_grief_undo")) {
                PreciousStones.log("Creating table: pstone_grief_undo");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_grief_undo` ("
                        + "`id` INTEGER PRIMARY KEY, "
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

            if (!core.existsTable("pstone_translocations")) {
                PreciousStones.log("Creating table: pstone_translocations");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_translocations` ("
                        + "`id` INTEGER PRIMARY KEY, "
                        + "`name` varchar(36) NOT NULL, "
                        + "`player_name` varchar(16) NOT NULL, "
                        + "`minx` int(11) default NULL, "
                        + "`maxx` int(11) default NULL, "
                        + "`miny` int(11) default NULL, "
                        + "`maxy` int(11) default NULL, "
                        + "`minz` int(11) default NULL, "
                        + "`maxz` int(11) default NULL, "
                        + "UNIQUE (`name`,`player_name`))");
            }

            if (!core.existsTable("pstone_storedblocks")) {
                PreciousStones.log("Creating table: pstone_storedblocks");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_storedblocks` ("
                        + "`id` INTEGER PRIMARY KEY,  "
                        + "`name` varchar(36) NOT NULL, "
                        + "`player_name` varchar(16) NOT NULL, "
                        + "`x` int(11) default NULL, "
                        + "`y` int(11) default NULL, "
                        + "`z` int(11) default NULL, "
                        + "`world` varchar(25) NOT NULL, "
                        + "`type_id` int(11) NOT NULL, "
                        + "`data` TINYINT NOT NULL, "
                        + "`sign_text` varchar(75) NOT NULL, "
                        + "`applied` bit default 0, `contents` TEXT NOT NULL, UNIQUE (`x`,`y`,`z`,`world`));");
            }

            if (!core.existsTable("pstone_players")) {
                PreciousStones.log("Creating table: pstone_players");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_players` ("
                        + "`id` bigint(20), "
                        + "`uuid` varchar(255) default NULL, "
                        + "`player_name` varchar(16) NOT NULL, "
                        + "`last_seen` bigint(20) default NULL, "
                        + "flags TEXT default NULL, "
                        + "PRIMARY KEY (`player_name`))");
            }

            if (!core.existsTable("pstone_snitches")) {
                PreciousStones.log("Creating table: pstone_snitches");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_snitches` ("
                        + "`id` bigint(20), "
                        + "`x` int(11) default NULL, "
                        + "`y` int(11) default NULL, "
                        + "`z` int(11) default NULL, "
                        + "`world` varchar(25) default NULL, "
                        + "`name` varchar(16) NOT NULL, "
                        + "`reason` varchar(20) default NULL, "
                        + "`details` varchar(50) default NULL, "
                        + "`count` int(11) default NULL, "
                        + "`date` varchar(25) default NULL, "
                        + "PRIMARY KEY (`x`, `y`, `z`, `world`, `name`, `reason`, `details`))");

                addIndexes();
            }

            if (!core.existsTable("pstone_purchase_payments")) {
                PreciousStones.log("Creating table: pstone_purchase_payments");

                core.execute(
                        "CREATE TABLE IF NOT EXISTS `pstone_purchase_payments` ("
                        + "`id` bigint(20), "
                        + "`buyer` varchar(16) default NULL, "
                        + "`owner` varchar(16) NOT NULL, "
                        + "`item` varchar(20) default NULL, "
                        + "`amount` int(11) default NULL, "
                        + "`fieldName` varchar(255) default NULL, "
                        + "`coords` varchar(255) default NULL)");

                addIndexes();
            }
        }

        if (plugin.getSettingsManager().getVersion() < 9) {
            addData();
            plugin.getSettingsManager().setVersion(9);
        }

        if (plugin.getSettingsManager().getVersion() < 10) {
            addSnitchDate();
            plugin.getSettingsManager().setVersion(10);
        }

        if (plugin.getSettingsManager().isUseMysql()) {
            if (plugin.getSettingsManager().getVersion() < 12) {
                resetLastSeen();
                plugin.getSettingsManager().setVersion(12);
            }
        }

        if (!core.existsColumn("pstone_players", "uuid")) {
            updateUUID();
            addIndexes();
        }
    }

    /**
     * Bukkit 1.7.5+ UUID updates
     *
     * @param
     */
    private void updateUUID() {
        String query;

        query = "ALTER TABLE `pstone_players` ADD `uuid` VARCHAR( 255 ) DEFAULT NULL;";
        core.execute(query);

        PreciousStones.log("Added UUID modification to database");
    }

    public void addIndexes() {
        String query;

        if (plugin.getSettingsManager().isUseMysql()) {
            query = "ALTER TABLE `pstone_grief_undo` ADD UNIQUE KEY `key_grief_locs` (`x`, `y`, `z`, `world`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_fields` ADD INDEX `indx_field_owner` (`owner`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_players` ADD UNIQUE `unq_uuid` (uuid);";
            core.execute(query);

            query = "ALTER TABLE `pstone_players` ADD INDEX `inx_player_name` (player_name);";
            core.execute(query);

            query = "ALTER TABLE `pstone_cuboids` ADD INDEX `indx_cuboids_owner` (`owner`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_cuboids` ADD INDEX `indx_cuboids_parent` (`parent`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_unbreakables` ADD INDEX `indx_unbreakables_owner` (`owner`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_storedblocks` ADD INDEX `indx_storedblocks_1` (`name`, `player_name`, `applied`);";
            core.execute(query);

            query = "ALTER TABLE `pstone_storedblocks` ADD INDEX `indx_storedblocks_2` (`name`, `player_name`, `applied`, `type_id`, `data`);";
            core.execute(query);
        } else {
            query = "CREATE INDEX IF NOT EXISTS `indx_field_owner` ON `pstone_fields` (`owner`);";
            core.execute(query);

            query = "CREATE UNIQUE INDEX IF NOT EXISTS `indx_players_uuid` ON `pstone_players` (`uuid`);";
            core.execute(query);

            query = "CREATE UNIQUE INDEX IF NOT EXISTS `indx_player_name` ON `pstone_players` (`player_name`);";
            core.execute(query);

            query = "CREATE INDEX IF NOT EXISTS `indx_cuboids_owner` ON `pstone_cuboids` (`owner`);";
            core.execute(query);

            query = "CREATE INDEX IF NOT EXISTS `indx_cuboids_parent` ON `pstone_cuboids` (`parent`);";
            core.execute(query);

            query = "CREATE INDEX IF NOT EXISTS `indx_unbreakables_owner` ON `pstone_unbreakables` (`owner`);";
            core.execute(query);
        }

        PreciousStones.log("Added new indexes to database");
    }

    private void resetLastSeen() {
        PreciousStones.log("Updating last seen dates to new time format");

        if (!core.getDataType("pstone_grief_undo", "date_griefed").equals("bigint")) {
            core.execute("alter table pstone_grief_undo modify date_griefed bigint");
            core.execute("update pstone_grief_undo date_griefed = " + Helper.getMillis());
        }

        if (!core.getDataType("pstone_fields", "last_used").equals("bigint")) {
            core.execute("alter table pstone_fields modify last_used bigint");
            core.execute("update pstone_fields last_used = " + Helper.getMillis());
        }

        if (!core.getDataType("pstone_cuboids", "last_used").equals("bigint")) {
            core.execute("alter table pstone_cuboids modify last_used bigint");
            core.execute("update pstone_cuboids last_used = " + Helper.getMillis());
        }

        if (!core.getDataType("pstone_players", "last_seen").equals("bigint")) {
            core.execute("alter table pstone_players modify last_seen bigint");
            core.execute("update pstone_players last_seen = " + Helper.getMillis());
        }
    }

    private void addData() {
        if (!core.getDataType("pstone_fields", "data").equals("tinyint")) {
            core.execute("alter table pstone_fields add column data tinyint default 0");
        }

        if (!core.getDataType("pstone_cuboids", "data").equals("tinyint")) {
            core.execute("alter table pstone_cuboids add column data tinyint default 0");
        }

        if (!core.getDataType("pstone_unbreakables", "data").equals("tinyint")) {
            core.execute("alter table pstone_unbreakables add column data tinyint default 0");
        }
    }

    private void addSnitchDate() {
        if (!core.getDataType("pstone_snitches", "date").equals("varchar")) {
            core.execute("alter table pstone_snitches add column date varchar(25) default NULL");
        }
    }

    /**
     * Closes DB connection
     */
    public void closeConnection() {
        core.close();
    }

    /**
     * Load all pstones for any world that is loaded
     */
    public void loadWorldData() {
        PreciousStones.debug("finalizing queue");
        plugin.getForceFieldManager().offerAllDirtyFields();
        processQueue();

        PreciousStones.debug("clearing fields from memory");
        plugin.getForceFieldManager().clearChunkLists();
        plugin.getUnbreakableManager().clearChunkLists();

        final List<World> worlds = plugin.getServer().getWorlds();

        plugin.getServer().getScheduler().runTaskLaterAsynchronously(plugin, () -> {

            PreciousStones.debug("loading fields by world");

            for (World world : worlds) {
                loadWorldFields(world);
                loadWorldUnbreakables(world);
            }
        }, 0);
    }

    /**
     * Loads all fields for a specific world into memory
     *
     * @param world the world to load
     */
    public void loadWorldFields(World world) {
        int fieldCount;
        int cuboidCount;

        List<Field> fields;

        synchronized (this) {
            fields = getFields(world.getName());
            fieldCount = fields.size();
            Collection<Field> cuboids = getCuboidFields(world.getName());
            cuboidCount = cuboids.size();
            fields.addAll(cuboids);
        }

        for (Field field : fields) {
            // add to collection

            plugin.getForceFieldManager().addToCollection(field);

            // register grief reverts

            if (field.hasFlag(FieldFlag.GRIEF_REVERT) && field.getRevertingModule().getRevertSecs() > 0) {
                plugin.getGriefUndoManager().register(field);
            }

            // set the initial applied status to the field form the database

            if (field.hasFlag(FieldFlag.TRANSLOCATION)) {
                if (field.isNamed()) {
                    boolean applied = isTranslocationApplied(field.getName(), field.getOwner());
                    field.setDisabled(!applied, true);

                    int count = totalTranslocationCount(field.getName(), field.getOwner());
                    field.getTranslocatingModule().setTranslocationSize(count);
                }
            }

            // start renter scheduler

            if (field.hasFlag(FieldFlag.RENTABLE) || field.hasFlag(FieldFlag.SHAREABLE)) {
                field.getRentingModule().scheduleNextRentUpdate();
            }
        }

        if (fieldCount > 0) {
            PreciousStones.log("countsFields", world.getName(), fieldCount);
        }

        if (cuboidCount > 0) {
            PreciousStones.log("countsCuboids", world.getName(), cuboidCount);
        }
    }

    public int enableAllFlags(String flagStr) {
        int changed = 0;
        List<Field> fields = new ArrayList<>();

        synchronized (this) {
            List<World> worlds = plugin.getServer().getWorlds();

            for (World world : worlds) {
                fields.addAll(getFields(world.getName()));
                fields.addAll(getCuboidFields(world.getName()));
            }
        }

        plugin.getForceFieldManager().clearChunkLists();

        for (Field field : fields) {
            if (field.hasFlag(flagStr)) {
                changed++;
                field.getFlagsModule().disableFlag(flagStr, false);
                field.getFlagsModule().dirtyFlags("enableAllFlags");
            }

            plugin.getForceFieldManager().addToCollection(field);
        }

        return changed;
    }

    public int disableAllFlags(String flagStr) {
        int changed = 0;
        List<Field> fields = new ArrayList<>();

        synchronized (this) {
            List<World> worlds = plugin.getServer().getWorlds();

            for (World world : worlds) {
                fields.addAll(getFields(world.getName()));
                fields.addAll(getCuboidFields(world.getName()));
            }
        }

        plugin.getForceFieldManager().clearChunkLists();

        for (Field field : fields) {
            if (field.getFlagsModule().hasDisabledFlag(flagStr)) {
                changed++;
                field.getFlagsModule().enableFlag(flagStr);
                field.getFlagsModule().dirtyFlags("disableAllFlags");
            }

            plugin.getForceFieldManager().addToCollection(field);
        }

        return changed;
    }

    /**
     * Loads all unbreakables for a specific world into memory
     *
     * @param world
     */
    public void loadWorldUnbreakables(World world) {
        List<Unbreakable> unbreakables;

        synchronized (this) {
            unbreakables = getUnbreakables(world.getName());
        }

        for (Unbreakable ub : unbreakables) {
            plugin.getUnbreakableManager().addToCollection(ub);
        }

        if (!unbreakables.isEmpty()) {
            PreciousStones.log("countsUnbreakables", world, unbreakables.size());
        }
    }

    /**
     * Puts the field up for future storage
     *
     * @param field
     */
    public void offerField(Field field) {
        synchronized (pending) {
            pending.put(field.toVec(), field);
        }
    }

    /**
     * Puts the field up for grief reversion
     *
     * @param field
     */
    public void offerGrief(Field field) {
        synchronized (pendingGrief) {
            pendingGrief.add(field);
        }
    }


    /**
     * Puts the unbreakable up for future storage
     *
     * @param ub
     * @param insert
     */
    public void offerUnbreakable(Unbreakable ub, boolean insert) {
        synchronized (pendingUb) {
            pendingUb.put(ub, insert);
        }
    }

    /**
     * Puts the player up for future storage
     *
     * @param playerName
     */
    public void offerPlayer(String playerName) {
        synchronized (pendingPlayers) {
            pendingPlayers.put(playerName, true);
        }
    }

    /**
     * Puts the player up for future storage
     *
     * @param playerName
     */
    public void offerDeletePlayer(String playerName) {
        synchronized (pendingPlayers) {
            pendingPlayers.put(playerName, false);
        }
    }

    /**
     * Puts the snitch list up for future storage
     *
     * @param se
     */
    public void offerSnitchEntry(SnitchEntry se) {
        synchronized (pendingSnitchEntries) {
            pendingSnitchEntries.add(se);
        }
    }

    /**
     * Retrieves all fields belonging to a world from the database
     *
     * @param worldName
     * @return
     */
    public List<Field> getFields(String worldName) {
        List<Field> out = new ArrayList<>();
        boolean foundInWrongTable = false;

        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement(
                        "SELECT pstone_fields.id as id, x, y, z, radius, height, type_id, "
                                + "data, velocity, world, owner, name, packed_allowed, last_used, flags "
                                + "FROM pstone_fields WHERE world = ?")) {
            prepStmt.setString(1, worldName);

            try (ResultSet resultSet = prepStmt.executeQuery()) {
                while (resultSet.next()) {
                    try {
                        long id = resultSet.getLong("id");
                        int x = resultSet.getInt("x");
                        int y = resultSet.getInt("y");
                        int z = resultSet.getInt("z");
                        int radius = resultSet.getInt("radius");
                        int height = resultSet.getInt("height");
                        int type_id = resultSet.getInt("type_id");
                        float velocity = resultSet.getFloat("velocity");
                        String world = resultSet.getString("world");
                        String owner = resultSet.getString("owner");
                        String name = resultSet.getString("name");
                        String flags = resultSet.getString("flags");
                        String packed_allowed = resultSet.getString("packed_allowed");
                        long last_used = resultSet.getLong("last_used");

                        BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                        Field field = new Field(x, y, z, radius, height, velocity, world, type, owner, name, last_used);
                        field.setPackedAllowed(packed_allowed);
                        field.setId(id);

                        FieldSettings fs = plugin.getSettingsManager().getFieldSettings(field);

                        if (fs != null) {
                            field.setSettings(fs);
                            field.getFlagsModule().setFlags(flags);

                            if (fs.getAutoDisableTime() > 0) {
                                field.setDisabled(true, true);
                            }

                            out.add(field);

                            // check for fields in the wrong table

                            if (fs.hasDefaultFlag(FieldFlag.CUBOID)) {
                                deleteFieldFromBothTables(conn, field);
                                insertField(field);
                                foundInWrongTable = true;
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        if (foundInWrongTable) {
            System.err.println("[PreciousStones] Fields found in wrong table");
        }

        return out;
    }

    /**
     * Retrieves all of the cuboid fields belonging to a world from the database
     *
     * @param worldName
     * @return
     */
    public Collection<Field> getCuboidFields(String worldName) {
        HashMap<Long, Field> out = new HashMap<>();
        boolean foundInWrongTable = false;

        try (Connection conn = core.getConnection()) {
            try (PreparedStatement prepStmt = conn.prepareStatement(
                        "SELECT pstone_cuboids.id as id, x, y, z, minx, miny, minz, maxx, maxy, maxz, "
                        + "type_id, data, velocity, world, owner, name, packed_allowed, last_used, flags "
                        + "FROM  pstone_cuboids WHERE pstone_cuboids.parent = 0 AND world = ?")) {
                prepStmt.setString(1, worldName);

                try (ResultSet resultSet = prepStmt.executeQuery()) {
                    while (resultSet.next()) {
                        try {
                            long id = resultSet.getLong("id");
                            int x = resultSet.getInt("x");
                            int y = resultSet.getInt("y");
                            int z = resultSet.getInt("z");
                            int minx = resultSet.getInt("minx");
                            int miny = resultSet.getInt("miny");
                            int minz = resultSet.getInt("minz");
                            int maxx = resultSet.getInt("maxx");
                            int maxy = resultSet.getInt("maxy");
                            int maxz = resultSet.getInt("maxz");
                            int type_id = resultSet.getInt("type_id");
                            float velocity = resultSet.getFloat("velocity");
                            String world = resultSet.getString("world");
                            String owner = resultSet.getString("owner");
                            String name = resultSet.getString("name");
                            String flags = resultSet.getString("flags");
                            String packed_allowed = resultSet.getString("packed_allowed");
                            long last_used = resultSet.getLong("last_used");

                            BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                            Field field = new Field(x, y, z, minx, miny, minz, maxx, maxy, maxz, velocity, world, type, owner, name, last_used);
                            field.setPackedAllowed(packed_allowed);
                            field.setId(id);

                            FieldSettings fs = plugin.getSettingsManager().getFieldSettings(field);

                            if (fs != null) {
                                field.setSettings(fs);
                                field.getFlagsModule().setFlags(flags);

                                if (fs.getAutoDisableTime() > 0) {
                                    field.setDisabled(true, true);
                                }

                                out.put(id, field);

                                // check for fields in the wrong table

                                if (!fs.hasDefaultFlag(FieldFlag.CUBOID)) {
                                    deleteFieldFromBothTables(conn, field);
                                    insertField(field);
                                    foundInWrongTable = true;
                                }
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
            try (PreparedStatement prepStmt = conn.prepareStatement(
                    "SELECT pstone_cuboids.id as id, parent, x, y, z, minx, miny, minz, maxx, maxy, maxz, "
                    + "type_id, data, velocity, world, owner, name, packed_allowed, last_used, flags "
                    + "FROM pstone_cuboids WHERE pstone_cuboids.parent > 0 AND world = ?'")) {
                prepStmt.setString(1, worldName);

                try (ResultSet resultSet = prepStmt.executeQuery()) {
                    while (resultSet.next()) {
                        try {
                            long id = resultSet.getLong("id");
                            long parent = resultSet.getLong("parent");
                            int x = resultSet.getInt("x");
                            int y = resultSet.getInt("y");
                            int z = resultSet.getInt("z");
                            int minx = resultSet.getInt("minx");
                            int miny = resultSet.getInt("miny");
                            int minz = resultSet.getInt("minz");
                            int maxx = resultSet.getInt("maxx");
                            int maxy = resultSet.getInt("maxy");
                            int maxz = resultSet.getInt("maxz");
                            int type_id = resultSet.getInt("type_id");
                            float velocity = resultSet.getFloat("velocity");
                            String world = resultSet.getString("world");
                            String owner = resultSet.getString("owner");
                            String name = resultSet.getString("name");
                            String flags = resultSet.getString("flags");
                            String packed_allowed = resultSet.getString("packed_allowed");
                            long last_used = resultSet.getLong("last_used");

                            BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                            Field field = new Field(x, y, z, minx, miny, minz, maxx, maxy, maxz, velocity, world, type, owner, name, last_used);
                            field.setPackedAllowed(packed_allowed);

                            Field parentField = out.get(parent);

                            if (parentField != null) {
                                field.setParent(parentField);
                                parentField.addChild(field);
                            } else {
                                field.markForDeletion();
                                offerField(field);
                            }

                            field.setId(id);

                            FieldSettings fs = plugin.getSettingsManager().getFieldSettings(field);

                            if (fs != null) {
                                field.setSettings(fs);
                                field.getFlagsModule().setFlags(flags);
                                out.put(id, field);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        if (foundInWrongTable) {
            PreciousStones.log("fieldsInWrongTable");
        }

        return out.values();
    }

    public void migrate(String oldUsername, String newUsername) {
        plugin.getForceFieldManager().migrateUsername(oldUsername, newUsername);
        plugin.getUnbreakableManager().migrateUsername(oldUsername, newUsername);

        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt1 = conn.prepareStatement(
                        "UPDATE `pstone_storedblocks` SET player_name = ? WHERE player_name = ?");
                PreparedStatement prepStmt2 = conn.prepareStatement(
                        "UPDATE `pstone_translocations` SET player_name = ? WHERE player_name = ?")) {

            prepStmt1.setString(1, newUsername);
            prepStmt1.setString(2, oldUsername);

            prepStmt2.setString(1, newUsername);
            prepStmt2.setString(2, oldUsername);

            prepStmt1.execute();
            prepStmt2.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        PreciousStones.log("[Username Changed] From: " + oldUsername + " To: " + newUsername);

        Player player = plugin.getServer().getPlayerExact(newUsername);

        if (player != null) {
            ChatHelper.send(player, "usernameChanged");
        }
    }

    public void deletePlayerAndData(String playerName) {
        int purged = plugin.getForceFieldManager().deleteBelonging(playerName);

        if (purged > 0) {
            PreciousStones.log("countsPurgedFields", playerName, purged);
        }

        purged = plugin.getUnbreakableManager().deleteBelonging(playerName);

        if (purged > 0) {
            PreciousStones.log("countsPurgedUnbreakables", playerName, purged);
        }

        offerDeletePlayer(playerName);
    }

    private PlayerEntry extractPlayer(ResultSet resultSet) throws SQLException {
        if (!resultSet.next()) {
            return null;
        }
        PlayerEntry data = new PlayerEntry();
        String uuid = resultSet.getString("uuid");

        // I am not sure how, but I managed to get "null" as a string in my player data
        if (uuid != null && uuid.equalsIgnoreCase("null")) {
            uuid = null;
        }

        String name = resultSet.getString("player_name");
        long last_seen = resultSet.getLong("last_seen");
        String flags = resultSet.getString("flags");

        if (last_seen > 0) {
            ZonedDateTime lastUsedDate = Instant.ofEpochMilli(last_seen).atZone(ZoneId.systemDefault());
            ZonedDateTime now = LocalDateTime.now().atZone(ZoneId.systemDefault());
            int lastSeenDays = (int)DAYS.between(lastUsedDate, now);
            PreciousStones.debug("Player last seen: %s [%s]", lastSeenDays, name);
        }

        data.setName(name);
        data.setFlags(flags);

        if (uuid != null) {
            data.setOnlineUUID(UUID.fromString(uuid));
        } else {
            UUID pulledUUID = UUIDMigration.findPlayerUUID(name);
            if (pulledUUID != null) {
                data.setOnlineUUID(pulledUUID);
                PreciousStones.log("[Online UUID Found] Player: " + name + " UUID: " + pulledUUID.toString());
                plugin.getStorageManager().updatePlayerUUID(name, pulledUUID);
            }
        }

        return data;
    }

    /**
     * Retrieves a player from the database
     */
    public PlayerEntry extractPlayer(String playerName) {
        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement(
                        "SELECT * FROM pstone_players WHERE player_name = ?")) {

            prepStmt.setString(1, playerName);
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                return extractPlayer(resultSet);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Retrieves a player from the database by UUID, may migrate data if needed
     */
    public PlayerEntry extractPlayer(UUID uuid) {
        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement(
                        "SELECT * FROM pstone_players WHERE uuid = ?")) {

            prepStmt.setString(1, uuid.toString());
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                return extractPlayer(resultSet);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public PlayerEntry createPlayer(String playerName, UUID uuid) {
        PlayerEntry data = new PlayerEntry();
        data.setName(playerName);
        data.setOnlineUUID(uuid);
        PreciousStones.log("[New Player]: " + playerName + " UUID: " + uuid);
        return data;
    }

    /**
     * Purge players from the database
     */
    public void purgePlayers() {
        int purgeDays = plugin.getSettingsManager().getPurgeAfterDays();
        long lastSeen = LocalDateTime.now().atZone(ZoneId.systemDefault()).minusDays(purgeDays).toInstant().toEpochMilli();

        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement(
                        "SELECT player_name FROM pstone_players WHERE last_seen < ?")) {
            
            prepStmt.setLong(1, lastSeen);
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString("player_name");

                    deletePlayerAndData(name);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Retrieves all unbreakables belonging to a worlds from the database
     *
     * @param worldName
     * @return
     */
    public List<Unbreakable> getUnbreakables(String worldName) {
        List<Unbreakable> out = new ArrayList<>();

        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement("SELECT * FROM  `pstone_unbreakables` WHERE world = ?")) {

            prepStmt.setString(1, worldName);
            try (ResultSet resultSet = prepStmt.executeQuery()) {
                while (resultSet.next()) {

                    int x = resultSet.getInt("x");
                    int y = resultSet.getInt("y");
                    int z = resultSet.getInt("z");
                    int type_id = resultSet.getInt("type_id");
                    String world = resultSet.getString("world");
                    String owner = resultSet.getString("owner");

                    BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                    Unbreakable ub = new Unbreakable(x, y, z, world, type, owner);

                    out.add(ub);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return out;
    }

    private void updateGrief(Field field) {
        if (field.isDirty(DirtyFieldReason.GRIEF_BLOCKS)) {
            Queue<GriefBlock> grief = field.getRevertingModule().getGrief();

            for (GriefBlock gb : grief) {
                insertBlockGrief(field, gb);
            }
        }
    }

    public void updateField(Field field) {
        DynamicQueryCreator creator = new DynamicQueryCreator();

        if (field.isDirty(DirtyFieldReason.OWNER)) {
            creator.add("owner = ?", field.getOwner());
        }
        if (field.isDirty(DirtyFieldReason.RADIUS)) {
            creator.add("radius = ?", field.getRadius());
        }
        if (field.isDirty(DirtyFieldReason.HEIGHT)) {
            creator.add("height = ?", field.getHeight());
        }
        if (field.isDirty(DirtyFieldReason.VELOCITY)) {
            creator.add("velocity = ?", field.getVelocity());
        }
        if (field.isDirty(DirtyFieldReason.NAME)) {
            creator.add("name = ", field.getName());
        }
        if (field.isDirty(DirtyFieldReason.ALLOWED)) {
            creator.add("packed_allowed = ?", field.getPackedAllowed());
        }
        if (field.isDirty(DirtyFieldReason.LASTUSED)) {
            creator.add("last_used = ?", Helper.getMillis());
        }
        if (field.isDirty(DirtyFieldReason.FLAGS)) {
            creator.add("flags = ?", field.getFlagsModule().getFlagsAsString());
        }
        if (field.isDirty(DirtyFieldReason.DIMENSIONS)) {
            creator.add("minx = ?, miny = ?, minz = ?, maxx = ?, maxy = ?, maxz = ?",
                    field.getMinx(), field.getMiny(), field.getMinz(), field.getMaxx(), field.getMaxy(), field.getMaxz());
        }
        String fieldUpdates = creator.toQueryString();
        if (fieldUpdates.isEmpty()) {
            return;
        }
        try (Connection conn = core.getConnection()) {
            try (PreparedStatement prepStmt = conn.prepareStatement(
                    "UPDATE `pstone_fields` SET " + fieldUpdates + " "
                    + "WHERE x = ? AND y = ? AND z = ? AND world = ?")) {
                int setCount = creator.setParameters(prepStmt, 0);
                prepStmt.setInt(++setCount, field.getX());
                prepStmt.setInt(++setCount, field.getY());
                prepStmt.setInt(++setCount, field.getZ());
                prepStmt.setString(++setCount, field.getWorld());
                prepStmt.execute();
            }
            if (field.hasFlag(FieldFlag.CUBOID)) {
                try (PreparedStatement prepStmt = conn.prepareStatement(
                        "UPDATE `pstone_cuboids` SET " + fieldUpdates + " "
                        + "WHERE x = ? AND y = ? AND z = ? AND world = ?")) {
                    int setCount = creator.setParameters(prepStmt, 0);
                    prepStmt.setInt(++setCount, field.getX());
                    prepStmt.setInt(++setCount, field.getY());
                    prepStmt.setInt(++setCount, field.getZ());
                    prepStmt.setString(++setCount, field.getWorld());
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        field.clearDirty();
    }

    /**
     * Insert a field into the database
     *
     * @param field
     */
    public void insertField(Field field) {
        Vec vec = field.toVec();

        if (pending.containsKey(vec)) {
            processSingleField(pending.get(field.toVec()));
        }

        String query;
        Object[] parameters;
        if (field.hasFlag(FieldFlag.CUBOID)) {
            query = "INSERT INTO `pstone_cuboids` (`parent`, `x`, `y`, `z`, `world`, `minx`, `miny`, `minz`, "
                    + "`maxx`, `maxy`, `maxz`, `velocity`, `type_id`, `data`, `owner`, `name`, `packed_allowed`, "
                    + "`last_used`, `flags`) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            parameters = new Object[] {
                    ((field.getParent() == null) ? 0 : field.getParent().getId()), field.getX(), field.getY(), field.getZ(),
                    field.getWorld(), field.getMinx(), field.getMiny(), field.getMinz(), field.getMaxx(), field.getMaxy(), field.getMaxz(),
                    field.getVelocity(), Helper.getMaterialId(field.getMaterial()), 0, field.getOwner(), field.getName(),
                    field.getPackedAllowed(), Helper.getMillis(), field.getFlagsModule().getFlagsAsString()
            };
        } else {
            query = "INSERT INTO `pstone_fields` (`x`, `y`, `z`, `world`, `radius`, `height`, `velocity`, "
                    + "`type_id`, `data`, `owner`, `name`, `packed_allowed`, `last_used`, `flags`) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            parameters = new Object[] {
                    field.getX(), field.getY(), field.getZ(), field.getWorld(), field.getRadius(), field.getHeight(), field.getVelocity(),
                    Helper.getMaterialId(field.getMaterial()), 0, field.getOwner(), field.getName(), field.getPackedAllowed(), Helper.getMillis(),
                    field.getFlagsModule().getFlagsAsString()
            };
        }
        long id = 0;
        try (Connection conn = core.getConnection();
                PreparedStatement prepStmt = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            SqlUtils.setArguments(prepStmt, parameters);
            prepStmt.execute();

            try (ResultSet genKeys = prepStmt.getGeneratedKeys()) {
                if (genKeys.next()) {
                    id = genKeys.getLong(1);
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        synchronized (this) {
            field.setId(id);
        }
    }

    /**
     * Delete a field from the database
     *
     * @param field
     */
    public void deleteField(Field field) {

        String table = (field.hasFlag(FieldFlag.CUBOID)) ? "pstone_cuboids" : "pstone_fields";
        String query = "DELETE FROM `{table}` WHERE x = ? AND y = ? AND z = ? AND world = ?";
        query = query .replace("{table}", table);

        Object[] parameters = new Object[] {field.getX(), field.getY(), field.getZ(), field.getWorld()};

        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                        PreparedStatement prepStmt = conn.prepareStatement(query)) {
                    SqlUtils.setArguments(prepStmt, parameters);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Deletes the field/cuboid from both tables
     *
     * @param field
     * @throws SQLException 
     */
    public void deleteFieldFromBothTables(Connection conn, Field field) throws SQLException {
        try (PreparedStatement prepStmt1 = conn.prepareStatement(
                "DELETE FROM `pstone_fields` WHERE x = ? AND y = ? AND z = ? AND world = ?");
                PreparedStatement prepStmt2 = conn.prepareStatement(
                        "DELETE FROM `pstone_cuboids` WHERE x = ? AND y = ? AND z = ? AND world = ?")) {

            prepStmt1.setInt(1, field.getX());
            prepStmt1.setInt(2, field.getY());
            prepStmt1.setInt(3, field.getZ());
            prepStmt1.setString(4, field.getWorld());

            prepStmt2.setInt(1, field.getX());
            prepStmt2.setInt(2, field.getY());
            prepStmt2.setInt(3, field.getZ());
            prepStmt2.setString(4, field.getWorld());

            synchronized (this) {
                prepStmt1.execute();
                prepStmt2.execute();
            }
        }
    }


    /**
     * Delete a field from the database that a player owns
     *
     * @param playerName
     */
    public void deleteFields(String playerName) {
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "DELETE FROM `pstone_fields` WHERE owner = ?")) {
                    prepStmt.setString(1, playerName);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Delete a unbreakables from the database that a player owns
     *
     * @param playerName
     */
    public void deleteUnbreakables(String playerName) {
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "DELETE FROM `pstone_unbreakables` WHERE owner = ?")) {
                    prepStmt.setString(1, playerName);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Insert an unbreakable into the database
     *
     * @param ub
     */
    public void insertUnbreakable(Unbreakable ub) {
        Object[] parameters = new Object[] {ub.getX(), ub.getY(), ub.getZ(), ub.getWorld(),
                ub.getOwner(), Helper.getMaterialId(ub.getMaterial()), 0};
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "INSERT INTO `pstone_unbreakables` (`x`, `y`, `z`, `world`, `owner`, `type_id`, `data`) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                    SqlUtils.setArguments(prepStmt, parameters);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Delete an unbreakable from the database
     *
     * @param ub
     */
    public void deleteUnbreakable(Unbreakable ub) {
        Object[] parameters = new Object[] {ub.getX(), ub.getY(), ub.getZ(), ub.getWorld()};
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "DELETE FROM `pstone_unbreakables` WHERE x = ? AND y = ? AND z = ? AND world = ?")) {

                    SqlUtils.setArguments(prepStmt, parameters);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Insert a pending purchase into the database
     *
     * @param purchase
     */
    public void insertPendingPurchasePayment(PurchaseEntry purchase) {
        BlockTypeEntry item = purchase.getItem();
        String itemName = item == null ? null : item.toString();

        Object[] parameters = new Object[] {purchase.getId(), purchase.getBuyer(), purchase.getOwner(), itemName,
                purchase.getAmount(), purchase.getFieldName(), purchase.getCoords()};
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "INSERT INTO `pstone_purchase_payments` (`id`, `buyer`, `owner`, `item`, `amount`, `fieldName`, `coords`) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {

                    SqlUtils.setArguments(prepStmt, parameters);
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Delete an pending purchase from the database
     *
     * @param purchase
     */
    public void deletePendingPurchasePayment(PurchaseEntry purchase) {
        try {
            synchronized (this) {
                try (Connection conn = core.getConnection();
                    PreparedStatement prepStmt = conn.prepareStatement(
                            "DELETE FROM `pstone_purchase_payments` WHERE id = ?")) {

                    prepStmt.setInt(1, purchase.getId());
                    prepStmt.execute();
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Retrieves all snitches belonging to a worlds from the database
     *
     * @param owner
     * @return
     */
    public List<PurchaseEntry> getPendingPurchases(String owner) {
        List<PurchaseEntry> out = new ArrayList<>();

        String query = "SELECT * FROM  `pstone_purchase_payments` WHERE owner = '" + owner + "';";


        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int id = res.getInt("id");
                                String buyer = res.getString("buyer");
                                String item = res.getString("item");
                                String fieldName = res.getString("fieldName");
                                String coords = res.getString("coords");
                                int amount = res.getInt("amount");

                                out.add(new PurchaseEntry(id, buyer, owner, fieldName, coords, new BlockTypeEntry(item), amount));
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return out;
    }

    /**
     * Insert snitch entry into the database
     *
     * @param snitch
     * @param se
     */
    public void insertSnitchEntry(Field snitch, SnitchEntry se) {
        if (plugin.getSettingsManager().isUseMysql()) {
            String query = "INSERT INTO `pstone_snitches` (`x`, `y`, `z`, `world`, `name`, `reason`, `details`, `count`, `date`) ";
            String values = "VALUES ( " + snitch.getX() + "," + snitch.getY() + "," + snitch.getZ() + ",'" + Helper.escapeQuotes(snitch.getWorld()) + "','" + Helper.escapeQuotes(se.getName()) + "','" + Helper.escapeQuotes(se.getReason()) + "','" + Helper.escapeQuotes(se.getDetails()) + "',1, '" + Helper.getMillis() + "') ";
            String update = "ON DUPLICATE KEY UPDATE count = count+1;";

            synchronized (this) {
                core.insert(query + values + update);
            }
        } else {
            String query = "INSERT OR IGNORE INTO `pstone_snitches` (`x`, `y`, `z`, `world`, `name`, `reason`, `details`, `count`, `date`) ";
            String values = "VALUES ( " + snitch.getX() + "," + snitch.getY() + "," + snitch.getZ() + ",'" + Helper.escapeQuotes(snitch.getWorld()) + "','" + Helper.escapeQuotes(se.getName()) + "','" + Helper.escapeQuotes(se.getReason()) + "','" + Helper.escapeQuotes(se.getDetails()) + "',1, '" + Helper.getMillis() + "');";
            String update = "UPDATE `pstone_snitches` SET count = count+1;";

            synchronized (this) {
                core.insert(query + values + update);
            }
        }
    }

    /**
     * Delete all snitch entries for a snitch form the database
     *
     * @param snitch
     */
    public void deleteSnitchEntries(Field snitch) {
        String query = "DELETE FROM `pstone_snitches` WHERE x = " + snitch.getX() + " AND y = " + snitch.getY() + " AND z = " + snitch.getZ() + " AND world = '" + Helper.escapeQuotes(snitch.getWorld()) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Retrieves all snitches belonging to a worlds from the database
     *
     * @param snitch
     * @return
     */
    public List<SnitchEntry> getSnitchEntries(Field snitch) {
        List<SnitchEntry> workingSnitchEntries = new ArrayList<>();

        synchronized (pendingSnitchEntries) {
            workingSnitchEntries.addAll(pendingSnitchEntries);
            pendingSnitchEntries.clear();
        }

        processSnitches(workingSnitchEntries);

        List<SnitchEntry> out = new ArrayList<>();

        String query = "SELECT * FROM  `pstone_snitches` WHERE x = " + snitch.getX() + " AND y = " + snitch.getY() + " AND z = " + snitch.getZ() + " AND world = '" + Helper.escapeQuotes(snitch.getWorld()) + "' ORDER BY `date` DESC;";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                String name = res.getString("name");
                                String reason = res.getString("reason");
                                String details = res.getString("details");
                                int count = res.getInt("count");

                                SnitchEntry ub = new SnitchEntry(null, name, reason, details, count);

                                out.add(ub);
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return out;
    }

    /**
     * Delete a player from the players table
     *
     * @param playerName
     */
    public void deletePlayer(String playerName) {
        String query = "DELETE FROM `pstone_players` WHERE player_name = '" + playerName + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Update the player's last seen date on the database
     *
     * @param playerName
     */
    public void updatePlayer(String playerName) {
        long time = Helper.getMillis();

        PlayerEntry data = plugin.getPlayerManager().getPlayerEntry(playerName);

        if (plugin.getSettingsManager().isUseMysql()) {
            String query = "INSERT INTO `pstone_players` (`player_name`,  `uuid`,  `last_seen`, `flags`) ";
            String values = "VALUES ( '" + playerName + "', '" + data.getOnlineUUID() + "', " + time + ",'" + Helper.escapeQuotes(data.getFlags()) + "') ";
            String update = "ON DUPLICATE KEY UPDATE last_seen = " + time + ", flags = '" + Helper.escapeQuotes(data.getFlags()) + "'";

            synchronized (this) {
                core.insert(query + values + update);
            }
        } else {
            String query = "INSERT OR IGNORE INTO `pstone_players` ( `player_name`,  `uuid`,  `last_seen`, `flags`) ";
            String values = "VALUES ( '" + playerName + "', '" + data.getOnlineUUID() + "', " + time + ",'" + Helper.escapeQuotes(data.getFlags()) + "');";
            String update = "UPDATE `pstone_players` SET last_seen = " + time + ", flags = '" + Helper.escapeQuotes(data.getFlags()) + "' WHERE player_name = '" + playerName + "';";

            synchronized (this) {
                core.insert(query + values + update);
            }
        }
    }

    /**
     * Update the player's uuid
     *
     * @param playerName
     */
    public void updatePlayerUUID(String playerName, UUID uuid) {
        String update = "UPDATE `pstone_players` SET `uuid` = '" + uuid.toString() + "' WHERE `player_name` = '" + playerName + "';";

        synchronized (this) {
            core.update(update);
        }
    }

    /**
     * Record a single block grief
     *
     * @param field
     * @param gb
     */
    public void insertBlockGrief(Field field, GriefBlock gb) {
        String query = "INSERT INTO `pstone_grief_undo` ( `date_griefed`, `field_x`, `field_y` , `field_z`, `world`, `x` , `y`, `z`, `type_id`, `data`, `sign_text`) ";
        String values = "VALUES ( '" + Helper.getMillis() + "'," + field.getX() + "," + field.getY() + "," + field.getZ() + ",'" + Helper.escapeQuotes(field.getWorld()) + "'," + gb.getX() + "," + gb.getY() + "," + gb.getZ() + "," + Helper.getMaterialId(gb.getType()) + "," + 0 + ",'" + Helper.escapeQuotes(gb.getSignText()) + "');";

        synchronized (this) {
            core.insert(query + values);
        }
    }

    /**
     * Restores a field's griefed blocks
     *
     * @param field
     * @return
     */
    public Queue<GriefBlock> retrieveBlockGrief(Field field) {
        synchronized (this) {
            haltUpdates = true;
        }

        Set<Field> workingGrief = new HashSet<>();

        synchronized (pendingGrief) {
            workingGrief.addAll(pendingGrief);
            pendingGrief.clear();
        }

        processGrief(workingGrief);

        Queue<GriefBlock> out = new LinkedList<>();

        String query = "SELECT * FROM  `pstone_grief_undo` WHERE field_x = " + field.getX() + " AND field_y = " + field.getY() + " AND field_z = " + field.getZ() + " AND world = '" + Helper.escapeQuotes(field.getWorld()) + "' ORDER BY y ASC;";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int x = res.getInt("x");
                                int y = res.getInt("y");
                                int z = res.getInt("z");
                                int type_id = res.getInt("type_id");
                                String signText = res.getString("sign_text");

                                BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                                GriefBlock gb = new GriefBlock(x, y, z, field.getWorld(), type);

                                if (type_id == 0 || type_id == 8 || type_id == 9 || type_id == 10 || type_id == 11) {
                                    gb.setEmpty(true);
                                }

                                gb.setSignText(signText);
                                out.add(gb);
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    PreciousStones.debug("Extracted %s griefed blocks from the db", out.size());

                    if (!out.isEmpty()) {
                        PreciousStones.debug("Deleting grief from the db");
                        deleteBlockGrief(field);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            haltUpdates = false;
        }


        return out;
    }

    /**
     * Deletes all records from a specific field
     *
     * @param field
     */
    public void deleteBlockGrief(Field field) {
        synchronized (pendingGrief) {
            pendingGrief.remove(field);
        }

        String query = "DELETE FROM `pstone_grief_undo` WHERE field_x = " + field.getX() + " AND field_y = " + field.getY() + " AND field_z = " + field.getZ() + " AND world = '" + Helper.escapeQuotes(field.getWorld()) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes all records from a specific block
     *
     * @param block
     */
    public void deleteBlockGrief(Block block) {
        String query = "DELETE FROM `pstone_grief_undo` WHERE x = " + block.getX() + " AND y = " + block.getY() + " AND z = " + block.getZ() + " AND world = '" + Helper.escapeQuotes(block.getWorld().getName()) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Checks if the translocation head record exists
     *
     * @param name
     * @param playerName
     * @return
     */
    public boolean existsTranslocatior(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM `pstone_translocations` WHERE `name` ='" + Helper.escapeQuotes(name) + "' AND `player_name` = '" + Helper.escapeQuotes(playerName) + "'";
        boolean exists = false;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            exists = res.getInt(1) > 0;
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return exists;
    }

    /**
     * Sets the size of the field
     *
     * @param field
     * @param fieldName
     * @return
     */
    public boolean changeSizeTranslocatiorField(Field field, String fieldName) {
        String query = "SELECT * FROM `pstone_translocations` WHERE `name` ='" + Helper.escapeQuotes(fieldName) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' LIMIT 1";
        boolean exists = false;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                field.setRelativeCuboidDimensions(res.getInt("minx"), res.getInt("miny"), res.getInt("minz"), res.getInt("maxx"), res.getInt("maxy"), res.getInt("maxz"));
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return exists;
    }

    /**
     * Add the head record for the translocation
     *
     * @param field
     * @param name
     */
    public void insertTranslocationHead(Field field, String name) {
        boolean exists = existsTranslocatior(name, field.getOwner());

        if (exists) {
            return;
        }

        String query = "INSERT INTO `pstone_translocations` ( `name`, `player_name`, `minx`, `miny`, `minz`, `maxx`, `maxy`, `maxz`) ";
        String values = "VALUES ( '" + Helper.escapeQuotes(name) + "','" + Helper.escapeQuotes(field.getOwner()) + "'," + field.getRelativeMin().getBlockX() + "," + field.getRelativeMin().getBlockY() + "," + field.getRelativeMin().getBlockZ() + "," + field.getRelativeMax().getBlockX() + "," + field.getRelativeMax().getBlockY() + "," + field.getRelativeMax().getBlockZ() + ");";

        synchronized (this) {
            core.insert(query + values);
        }
    }

    /**
     * Record a single translocation block
     *
     * @param field
     * @param tb
     */
    public void insertTranslocationBlock(Field field, TranslocationBlock tb) {
        insertTranslocationBlock(field, tb, true);
    }

    /**
     * Record a single translocation block
     *
     * @param field
     * @param tb
     * @param applied
     */
    public void insertTranslocationBlock(Field field, TranslocationBlock tb, boolean applied) {
        String query = "INSERT INTO `pstone_storedblocks` ( `name`, `player_name`, `world`, `x` , `y`, `z`, `type_id`, `data`, `contents`, `sign_text`, `applied`) ";
        String values = "VALUES ( '" + Helper.escapeQuotes(field.getName()) + "','" + Helper.escapeQuotes(field.getOwner()) + "','" + Helper.escapeQuotes(field.getWorld()) + "'," + tb.getRx() + "," + tb.getRy() + "," + tb.getRz() + "," + Helper.getMaterialId(tb.getType()) + "," + 0 + ",'" + tb.getContents() + "','" + Helper.escapeQuotes(tb.getSignText()) + "', " + (applied ? 1 : 0) + ");";

        synchronized (this) {
            core.insert(query + values);
        }
    }

    /**
     * Retrieves the count of applied translocation blocks
     *
     * @param field
     * @return
     */
    public int appliedTranslocationCount(Field field) {
        return appliedTranslocationCount(field.getName(), field.getOwner());
    }

    /**
     * Retrieves the count of applied translocation blocks
     *
     * @param name
     * @param playerName
     * @return
     */
    public int appliedTranslocationCount(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(name) + "' AND `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `applied` = 1";
        int count = 0;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            count = res.getInt(1);
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    /**
     * Retrieves the count of applied translocation blocks
     *
     * @param name
     * @param playerName
     * @return
     */
    public int totalTranslocationCount(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(name) + "' AND `player_name` = '" + Helper.escapeQuotes(playerName) + "'";
        int count = 0;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            count = res.getInt(1);
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    /**
     * Retrieves the count of unapplied translocation blocks
     *
     * @param field
     * @return
     */
    public int unappliedTranslocationCount(Field field) {
        return unappliedTranslocationCount(field.getName(), field.getOwner());
    }

    /**
     * Retrieves the count of unapplied translocation blocks
     *
     * @param name
     * @param playerName
     * @return
     */
    public int unappliedTranslocationCount(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(name) + "' AND `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `applied` = 0";
        int count = 0;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            count = res.getInt(1);
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return count;
    }

    /**
     * Returns the translocation blocks, and marks them as not-applied on the database
     *
     * @param field
     * @return
     */
    public Queue<TranslocationBlock> retrieveClearTranslocation(Field field) {
        Queue<TranslocationBlock> out = new LinkedList<>();

        String query = "SELECT * FROM  `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `applied` = 1 ORDER BY y ASC;";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int x = res.getInt("x");
                                int y = res.getInt("y");
                                int z = res.getInt("z");

                                World world = plugin.getServer().getWorld(field.getWorld());

                                Location location = new Location(world, x, y, z);
                                location = location.add(field.getLocation());

                                int type_id = res.getInt("type_id");
                                String signText = res.getString("sign_text");
                                String contents = res.getString("contents");

                                BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                                TranslocationBlock tb = new TranslocationBlock(location, type);

                                if (type_id == 0 || type_id == 8 || type_id == 9 || type_id == 10 || type_id == 11) {
                                    tb.setEmpty(true);
                                }

                                tb.setContents(contents);
                                tb.setRelativeCoords(x, y, z);
                                tb.setSignText(signText);
                                out.add(tb);
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        clearTranslocation(field);
        return out;
    }

    /**
     * Returns the translocation blocks, and marks them as not-applied on the database
     *
     * @param field
     * @return
     */
    public Queue<TranslocationBlock> retrieveTranslocation(Field field) {
        Queue<TranslocationBlock> out = new LinkedList<>();

        String query = "SELECT * FROM  `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `applied` = 0 ORDER BY y ASC;";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int x = res.getInt("x");
                                int y = res.getInt("y");
                                int z = res.getInt("z");

                                World world = plugin.getServer().getWorld(field.getWorld());

                                Location location = new Location(world, x, y, z);
                                location = location.add(field.getLocation());

                                int type_id = res.getInt("type_id");
                                String signText = res.getString("sign_text");
                                String contents = res.getString("contents");

                                BlockTypeEntry type = new BlockTypeEntry(Helper.getMaterial(type_id));

                                TranslocationBlock tb = new TranslocationBlock(location, type);

                                if (type_id == 0 || type_id == 8 || type_id == 9 || type_id == 10 || type_id == 11) {
                                    tb.setEmpty(true);
                                }

                                tb.setContents(contents);
                                tb.setRelativeCoords(x, y, z);
                                tb.setSignText(signText);
                                out.add(tb);
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        applyTranslocation(field);
        return out;
    }

    /**
     * Returns the players stored translocations and their sizes
     *
     * @param playerName
     * @return
     */
    public Map<String, Integer> getTranslocationDetails(String playerName) {
        Map<String, Integer> out = new HashMap<>();

        String query = "SELECT name, COUNT(name) FROM  `pstone_storedblocks` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "' GROUP BY `name`;";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {
                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                String name = res.getString(1);
                                int count = res.getInt(2);

                                out.put(name, count);
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return out;
    }

    /**
     * Returns whether a field with that name by that player exists
     *
     * @param name
     * @param playerName
     * @return
     */
    public boolean existsFieldWithName(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM  `pstone_fields` WHERE `owner` = '" + Helper.escapeQuotes(playerName) + "' AND `name` ='" + Helper.escapeQuotes(name) + "'";
        boolean exists = false;

        synchronized (this) {
            try {
                try (ResultSet res = core.select(query)) {

                    if (res != null) {
                        try {
                            while (res.next()) {
                                try {
                                    int count = res.getInt(1);
                                    exists = count > 0;
                                } catch (Exception ex) {
                                    PreciousStones.getLog().info(ex.getMessage());
                                }
                            }
                        } catch (SQLException ex) {
                            Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        query = "SELECT COUNT(*) FROM  `pstone_cuboids` WHERE `owner` = '" + Helper.escapeQuotes(playerName) + "' AND `name` ='" + Helper.escapeQuotes(name) + "'";

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int count = res.getInt(1);
                                exists = exists || count > 0;
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return exists;
    }

    /**
     * Returns whether there is data witht tha name for that player
     *
     * @param name
     * @param playerName
     * @return
     */
    public boolean existsTranslocationDataWithName(String name, String playerName) {
        String query = "SELECT COUNT(*) FROM  `pstone_storedblocks` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `name` ='" + Helper.escapeQuotes(name) + "'";
        boolean exists = false;

        synchronized (this) {
            try (ResultSet res = core.select(query)) {

                if (res != null) {
                    try {
                        while (res.next()) {
                            try {
                                int count = res.getInt(1);
                                exists = count > 0;
                            } catch (Exception ex) {
                                PreciousStones.getLog().info(ex.getMessage());
                            }
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(StorageManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return exists;
    }

    /**
     * Marks all translocation blocks as applied for a given field
     *
     * @param field
     */
    public void applyTranslocation(Field field) {
        String query = "UPDATE `pstone_storedblocks` SET `applied` = 1 WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `applied` = 0;";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Marks all translocation blocks as not-applied for a given field
     *
     * @param field
     */
    public void clearTranslocation(Field field) {
        String query = "UPDATE `pstone_storedblocks` SET `applied` = 0 WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `applied` = 1;";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Deletes all records from a specific field
     *
     * @param field
     */
    public void deleteAppliedTranslocation(Field field) {
        String query = "DELETE FROM `pstone_storedblocks` WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `applied` = 1;";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes a specific block from a translocation field
     *
     * @param field
     * @param tb
     */
    public void deleteTranslocation(Field field, TranslocationBlock tb) {
        Location location = tb.getRelativeLocation();

        String query = "DELETE FROM `pstone_storedblocks` WHERE x = " + location.getBlockX() + " AND y = " + location.getBlockY() + " AND z = " + location.getBlockZ() + " AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `name` = '" + Helper.escapeQuotes(field.getName()) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes all records from a player
     *
     * @param playerName
     */
    public void deleteTranslocation(String playerName) {
        String query = "DELETE FROM `pstone_storedblocks` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes all of a translocation's blocks
     *
     * @param name
     * @param playerName
     */
    public void deleteTranslocation(String name, String playerName) {
        String query = "DELETE FROM `pstone_storedblocks` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `name` = '" + Helper.escapeQuotes(name) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes the translocation's head record
     *
     * @param name
     * @param playerName
     */
    public void deleteTranslocationHead(String name, String playerName) {
        String query = "DELETE FROM `pstone_translocations` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `name` = '" + Helper.escapeQuotes(name) + "';";

        synchronized (this) {
            core.delete(query);
        }
    }

    /**
     * Deletes the translocation's head record
     *
     * @param name
     * @param playerName
     * @param block
     * @return
     */
    public int deleteBlockTypeFromTranslocation(String name, String playerName, BlockTypeEntry block) {
        int beforeCount = totalTranslocationCount(name, playerName);

        String query = "DELETE FROM `pstone_storedblocks` WHERE `player_name` = '" + Helper.escapeQuotes(playerName) + "' AND `name` = '" + Helper.escapeQuotes(name) + "' AND `type_id` = " + Helper.getMaterialId(block.getMaterial()) + ";";

        synchronized (this) {
            core.delete(query);
        }

        int afterCount = totalTranslocationCount(name, playerName);

        return beforeCount - afterCount;
    }

    /**
     * Changes the owner of a translocation block
     *
     * @param field
     * @param newOwner
     */
    public void changeTranslocationOwner(Field field, String newOwner) {
        String query = "UPDATE `pstone_storedblocks` SET `player_name` = '" + Helper.escapeQuotes(newOwner) + "' WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "';";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Mark a single translocation block as applied in a field
     *
     * @param field
     * @param tb
     * @param applied
     */
    public void updateTranslocationBlockApplied(Field field, TranslocationBlock tb, boolean applied) {
        Location location = tb.getRelativeLocation();

        String query = "UPDATE `pstone_storedblocks` SET `applied` = " + (applied ? 1 : 0) + " WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `x` = " + location.getBlockX() + " AND `y` = " + location.getBlockY() + " AND `z` = " + location.getBlockZ() + ";";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Returns whether the translocation is applied or not
     *
     * @param name
     * @param playerName
     * @return
     */
    public boolean isTranslocationApplied(String name, String playerName) {
        return appliedTranslocationCount(name, playerName) > 0;
    }

    /**
     * Update a block's content on the database
     *
     * @param field
     * @param tb
     */
    public void updateTranslocationBlockContents(Field field, TranslocationBlock tb) {
        Location location = tb.getRelativeLocation();

        String query = "UPDATE `pstone_storedblocks` SET `contents` = '" + tb.getContents() + "' WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `x` = " + location.getBlockX() + " AND `y` = " + location.getBlockY() + " AND `z` = " + location.getBlockZ() + ";";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Update a block's signtext on the database
     *
     * @param field
     * @param tb
     */
    public void updateTranslocationSignText(Field field, TranslocationBlock tb) {
        Location location = tb.getRelativeLocation();

        String query = "UPDATE `pstone_storedblocks` SET `sign_text` = '" + tb.getSignText() + "' WHERE `name` ='" + Helper.escapeQuotes(field.getName()) + "' AND `player_name` = '" + Helper.escapeQuotes(field.getOwner()) + "' AND `x` = " + location.getBlockX() + " AND `y` = " + location.getBlockY() + " AND `z` = " + location.getBlockZ() + ";";

        synchronized (this) {
            core.update(query);
        }
    }

    /**
     * Schedules the pending queue on save frequency
     *
     * @return
     */
    public BukkitTask saverScheduler() {
        return Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, () -> processQueue(), 0, 20L * plugin.getSettingsManager().getSaveFrequency());
    }

    /**
     * Process entire queue
     */
    public void processQueue() {
        synchronized (this) {
            if (haltUpdates) {
                return;
            }
        }

        Map<Vec, Field> working = new HashMap<>();
        Map<Unbreakable, Boolean> workingUb = new HashMap<>();
        Map<String, Boolean> workingPlayers = new HashMap<>();
        Set<Field> workingGrief = new HashSet<>();
        List<SnitchEntry> workingSnitchEntries = new ArrayList<>();

        synchronized (pending) {
            working.putAll(pending);
            pending.clear();
        }
        synchronized (pendingUb) {
            workingUb.putAll(pendingUb);
            pendingUb.clear();
        }
        synchronized (pendingGrief) {
            workingGrief.addAll(pendingGrief);
            pendingGrief.clear();
        }
        synchronized (pendingPlayers) {
            workingPlayers.putAll(pendingPlayers);
            pendingPlayers.clear();
        }
        synchronized (pendingSnitchEntries) {
            workingSnitchEntries.addAll(pendingSnitchEntries);
            pendingSnitchEntries.clear();
        }

        if (!working.isEmpty()) {
            processFields(working);
        }

        if (!workingUb.isEmpty()) {
            processUnbreakable(workingUb);
        }

        if (!workingGrief.isEmpty()) {
            processGrief(workingGrief);
        }

        if (!workingPlayers.isEmpty()) {
            processPlayers(workingPlayers);
        }

        if (!workingSnitchEntries.isEmpty()) {
            processSnitches(workingSnitchEntries);
        }
    }

    /**
     * Process suingle field
     *
     * @param field
     */
    public void processSingleField(Field field) {
        if (plugin.getSettingsManager().isDebug()) {
            PreciousStones.getLog().info("[Queue] processing single query");
        }

        if (field.isDirty(DirtyFieldReason.DELETE)) {
            deleteField(field);
        } else {
            updateField(field);
        }

        synchronized (this) {
            pending.remove(field.toVec());
        }
    }

    /**
     * Process pending pstones
     *
     * @param working
     */
    public void processFields(Map<Vec, Field> working) {
        if (plugin.getSettingsManager().isDebug() && !working.isEmpty()) {
            PreciousStones.getLog().info("[Queue] processing " + working.size() + " pstone queries...");
        }

        for (Field field : working.values()) {
            if (field.isDirty(DirtyFieldReason.DELETE)) {
                deleteField(field);
            } else {
                updateField(field);
            }
        }
    }

    /**
     * Process pending grief
     *
     * @param workingUb
     */
    public void processUnbreakable(Map<Unbreakable, Boolean> workingUb) {
        if (plugin.getSettingsManager().isDebug() && !workingUb.isEmpty()) {
            PreciousStones.getLog().info("[Queue] processing " + workingUb.size() + " unbreakable queries...");
        }

        for (Entry<Unbreakable, Boolean> ub : workingUb.entrySet()) {
            if (workingUb.get(ub.getValue())) {
                insertUnbreakable(ub.getKey());
            } else {
                deleteUnbreakable(ub.getKey());
            }
        }
    }

    /**
     * Process pending players
     *
     * @param workingPlayers
     */
    public void processPlayers(Map<String, Boolean> workingPlayers) {
        if (plugin.getSettingsManager().isDebug() && !workingPlayers.isEmpty()) {
            PreciousStones.getLog().info("[Queue] processing " + workingPlayers.size() + " player queries...");
        }

        for (String playerName : workingPlayers.keySet()) {
            if (workingPlayers.get(playerName)) {
                updatePlayer(playerName);
            } else {
                deletePlayer(playerName);
                deleteTranslocation(playerName);
                deleteFields(playerName);
                deleteUnbreakables(playerName);
            }
        }
    }

    /**
     * Process pending snitches
     *
     * @param workingSnitchEntries
     */
    public void processSnitches(List<SnitchEntry> workingSnitchEntries) {
        if (plugin.getSettingsManager().isDebug() && !workingSnitchEntries.isEmpty()) {
            PreciousStones.getLog().info("[Queue] sending " + workingSnitchEntries.size() + " snitch queries...");
        }

        for (SnitchEntry se : workingSnitchEntries) {
            insertSnitchEntry(se.getField(), se);
        }
    }

    /**
     * Process pending grief
     *
     * @param workingGrief
     */
    public void processGrief(Set<Field> workingGrief) {
        if (plugin.getSettingsManager().isDebug() && !workingGrief.isEmpty()) {
            PreciousStones.getLog().info("[Queue] processing " + workingGrief.size() + " grief queries...");
        }

        for (Field field : workingGrief) {
            updateGrief(field);
        }
    }
}
