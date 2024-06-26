package databasesystem.dto;

public class AttributeMetadataDTO {
    private final String attributeName;
    private final int position;
    private final int maxLength;

    public AttributeMetadataDTO(String attributeName, int position, int maxLength) {
        this.attributeName = attributeName;
        this.position = position;
        this.maxLength = maxLength;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public int getPosition() {
        return position;
    }

    public int getMaxLength() {
        return maxLength;
    }
}
