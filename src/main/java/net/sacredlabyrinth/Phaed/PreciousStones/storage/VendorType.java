package net.sacredlabyrinth.Phaed.PreciousStones.storage;

enum VendorType {

    MYSQL,
    SQLITE;
    
    /**
     * Whether this vendor supports the "INSERT INTO ... ON DUPLICATE KEY UPDATE" syntax
     * 
     * @return true if mysql, false otherwise
     */
    boolean supportsInsertOnDuplicateKeyUpdate() {
        return this == MYSQL;
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
