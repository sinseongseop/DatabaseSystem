package databasesystem.dto;

import java.util.LinkedList;
import java.util.List;

public class CreateInfoDTO {
    private final String tableName ;
    private int columnCount=0;
    List<AttributeDTO> attributes = new LinkedList<>();

    public CreateInfoDTO(String tableName) {
        this.tableName = tableName;
    }

    public void addAttribute(AttributeDTO attribute) {
        this.attributes.add(attribute);
        this.columnCount+=1;
    }

    public String getTableName() {
        return tableName;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public List<AttributeDTO> getAttributes() {
        return attributes;
    }

}
