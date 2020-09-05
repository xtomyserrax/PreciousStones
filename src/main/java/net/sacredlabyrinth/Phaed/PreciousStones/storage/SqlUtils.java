package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.sacredlabyrinth.Phaed.PreciousStones.field.Field;

final class SqlUtils {

    private SqlUtils() {}
    
    static void setArguments(PreparedStatement prepStmt, Object[] parameters) throws SQLException {
        for (int n = 0; n < parameters.length; n++) {
            prepStmt.setObject(n + 1, parameters[n]);
        }
    }
    
    static void setFieldCoordinates(PreparedStatement prepStmt, Field field) throws SQLException {
        setFieldCoordinates(prepStmt, field, 0);
    }
    
    static void setFieldCoordinates(PreparedStatement prepStmt, Field field, int offset) throws SQLException {
        prepStmt.setInt(offset + 1, field.getX());
        prepStmt.setInt(offset + 2, field.getY());
        prepStmt.setInt(offset + 3, field.getZ());
        prepStmt.setString(offset + 4, field.getWorld());
    }
    
    static void setFieldNameAndOwner(PreparedStatement prepStmt, Field field) throws SQLException {
        prepStmt.setString(1, field.getName());
        prepStmt.setString(2, field.getOwner());
    }
    
}
