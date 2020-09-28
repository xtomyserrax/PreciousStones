package net.sacredlabyrinth.Phaed.PreciousStones.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class DynamicQueryCreator {

    private final StringBuilder queryBuilder;
    private final List<Object> parameters;
    
    private DynamicQueryCreator(StringBuilder queryBuilder, List<Object> parameters) {
        this.queryBuilder = queryBuilder;
        this.parameters = parameters;
    }
    
    DynamicQueryCreator() {
        this(new StringBuilder(), new ArrayList<>());
    }
    
    DynamicQueryCreator(DynamicQueryCreator copyFrom) {
        this(new StringBuilder(copyFrom.queryBuilder.toString()), new ArrayList<>(copyFrom.parameters));
    }
    
    private void appendWithComma(String content) {
        if (queryBuilder.length() != 0) {
            queryBuilder.append(", ");
        }
        queryBuilder.append(content);
    }
    
    void add(String content, Object parameter) {
        appendWithComma(content);
        parameters.add(parameter);
    }
    
    void add(String content, Object...parameters) {
        appendWithComma(content);
        for (Object parameter : parameters) {
            this.parameters.add(parameter);
        }
    }
    
    String toQueryString() {
        return queryBuilder.toString();
    }
    
    /**
     * Sets all parameters in this query creator on the specified prepared statement
     * 
     * @param prepStmt the prepared statement
     * @param offset the offset at which to begin setting parameters
     * @return the amount of parameters set
     * @throws SQLException per JDBC, generally
     */
    int setParameters(PreparedStatement prepStmt, int offset) throws SQLException {
        for (int n = 0; n < parameters.size(); n++) {
            prepStmt.setObject(offset + 1 + n, parameters.get(n));
        }
        return parameters.size();
    }
    
}
