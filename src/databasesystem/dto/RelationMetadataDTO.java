package databasesystem.dto;

public class RelationMetadataDTO {
    private final String relationName;
    private final int attributeCnt;
    private final String fileLocation;

    public RelationMetadataDTO(String relationName, int attributeCnt, String fileLocation) {
        this.relationName = relationName;
        this.attributeCnt = attributeCnt;
        this.fileLocation = fileLocation;
    }

    public String getRelationName() {
        return relationName;
    }

    public int getAttributeCnt() {
        return attributeCnt;
    }

    public String getFileLocation() {
        return fileLocation;
    }
}
