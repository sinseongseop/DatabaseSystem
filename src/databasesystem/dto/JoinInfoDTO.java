package databasesystem.dto;

public class JoinInfoDTO {
    private final String tableName ;
    private final String AnotherTableName;
    private final String tableJoinColumn;
    private final String AnotherTableJoinColumn;

    public JoinInfoDTO(String tableName, String anotherTableName, String tableJoinColumn, String anotherTableJoinColumn) {
        this.tableName = tableName;
        AnotherTableName = anotherTableName;
        this.tableJoinColumn = tableJoinColumn;
        AnotherTableJoinColumn = anotherTableJoinColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAnotherTableName() {
        return AnotherTableName;
    }

    public String getTableJoinColumn() {
        return tableJoinColumn;
    }

    public String getAnotherTableJoinColumn() {
        return AnotherTableJoinColumn;
    }
}
