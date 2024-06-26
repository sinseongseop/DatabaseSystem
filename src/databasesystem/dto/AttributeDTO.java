package databasesystem.dto;

public class AttributeDTO {
    private final String attributeName;
    private final String attributeLength;

    public AttributeDTO(String attributeName, String attributeLength) {
        this.attributeName = attributeName;
        this.attributeLength = attributeLength;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getAttributeLength() {
        return attributeLength;
    }

}
