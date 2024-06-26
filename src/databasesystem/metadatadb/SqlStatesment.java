package databasesystem.metadatadb;

public class SqlStatesment {

    static final String INSERT_RELATION_META_SQL = "INSERT INTO relation_metadata (Relation_name, Number_of_attribute, File_location) VALUES (?, ?, ?)" ;
    static final String INSERT_ATTRIBUTE_META_SQL = "INSERT INTO attribute_metadata (Relation_name, Attribute_name, Positional, Length) VALUES (?, ?, ?, ?)";
    static final String INSERT_STATICS_META_SQL = "INSERT INTO statics_metadata (Relation_name, Record_size, Record_count) VALUES (?, ?, 0)";

    static final String SELECT_RELATION_META_SQL = "SELECT * FROM relation_metadata where Relation_name = ?";
    static final String SELECT_ATTRIBUTE_META_SQL = "SELECT * FROM attribute_metadata WHERE Relation_name = ? ORDER BY Positional ASC ";
    static final String SELECT_STATICS_META_SQL = "SELECT * FROM statics_metadata WHERE Relation_name = ?";

    static final String INCREASE_RECORDSIZE_SQL ="UPDATE statics_metadata SET Record_count = Record_count + 1 WHERE Relation_name = ?";
    static final String DECREASE_RECORDSIZE_SQL ="UPDATE statics_metadata SET Record_count = Record_count - 1 WHERE Relation_name = ?";

}
