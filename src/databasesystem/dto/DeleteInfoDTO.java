package databasesystem.dto;

public class DeleteInfoDTO {
    private final String tableName;
    private final String searchAttribute;
    private final String searchValue;

    public DeleteInfoDTO(String tableName, String searchAttribute, String searchValue) {
        this.tableName = tableName;
        this.searchAttribute = searchAttribute;
        this.searchValue = searchValue;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSearchAttribute() {
        return searchAttribute;
    }

    public String getSearchValue() {
        return searchValue;
    }
}
