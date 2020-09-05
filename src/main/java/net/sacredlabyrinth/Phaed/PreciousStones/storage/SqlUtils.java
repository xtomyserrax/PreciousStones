package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;

final class SqlUtils {

    private SqlUtils() {}
    
    static void setArguments(PreparedStatement prepStmt, Object[] parameters) throws SQLException {
        for (int n = 0; n < parameters.length; n++) {
            prepStmt.setObject(n + 1, parameters[n]);
        }
    }
    
}
