package databasesystem.dto;

import java.util.List;

public class InsertInfoDTO {
    private final String tableName ;
    private final List<String> values;

    public InsertInfoDTO(String tableName, List<String> values) {
        this.tableName = tableName;
        this.values = values;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getValues() {
        return values;
    }

}
