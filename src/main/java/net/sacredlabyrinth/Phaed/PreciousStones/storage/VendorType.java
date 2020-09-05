package net.sacredlabyrinth.Phaed.PreciousStones.storage;

enum VendorType {

    MYSQL,
    SQLITE;
    
    /**
     * MySQL and SQLite differ in syntax for "insert new row or update existing" queries. <br>
     * <br>
     * This method replaces <code>{insert}</code> and <code>{update}</code> in the query string accordingly.
     * For example, with MySQL, <code>insert</code> becomes "INSERT INTO" and <code>{update}</code> becomes
     * "ON DUPLICATE KEY UPDATE"
     * 
     * @param query the original query
     * @param tableName the table name, required for some vendors
     * @return the ready query string
     */
    String parseInsertOrUpdate(String query, String tableName) {
        switch (this) {
        case MYSQL:
            return query.replace("{insert}", "INSERT INTO").replace("{update}", "ON DUPLICATE KEY UPDATE");
        case SQLITE:
            return query.replace("{insert}", "INSERT OR IGNORE INTO").replace("{update}", "UPDATE " + tableName + " SET");
        default:
            throw new IllegalStateException("I do not know myself");
        }
    }
    
    /**
     * Whether the {@link DBCore#getDataType(String, String)} method returns a meaningful string
     * when using this vendor. If {@code false}, such method returns an empty string
     * 
     * @return true if data type retrieval is supported
     */
    boolean supportsDataTypeRetrieval() {
        return this != SQLITE;
    }
    
}
