package databasesystem.dto;

import java.util.List;

public class SelectInfoDTO {
    private final String tableName ;
    private final String searchAttribute;
    private final String searchValue;
    private final List<String> findAttributes;

    public SelectInfoDTO(String tableName, String searchAttribute, String searchValue, List<String> findAttributes)
    {
        this.tableName = tableName;
        this.searchAttribute = searchAttribute;
        this.searchValue = searchValue;
        this.findAttributes = findAttributes;
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

    public List<String> getFindAttributes() {
        return findAttributes;
    }
}
