package databasesystem.metadatadb;

import databasesystem.dto.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MetaDataProcessor {

    MetaDataDB metaDataDB;
    Connection conn;
    PreparedStatement statement;

    public MetaDataProcessor(){
        this.metaDataDB = new MetaDataDB();
        this.conn = metaDataDB.dbConnection();
    }

    //table을 create 할때 생성되는 모든 metaData를 metadataDB에 업데이트
    public void createMetadata(CreateInfoDTO createInfo){
        insertRelationMetadata(createInfo);
        insertAttributeMetadata(createInfo);
        insertStaticsMetadata(createInfo);
    }

    private void insertRelationMetadata(CreateInfoDTO createInfo) {
        String tableName = createInfo.getTableName();
        String filePath = tableName+".txt";

        try {
            statement = conn.prepareStatement(SqlStatesment.INSERT_RELATION_META_SQL);
            statement.setString(1, tableName);
            statement.setInt(2, createInfo.getColumnCount() );
            statement.setString(3, filePath);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertAttributeMetadata(CreateInfoDTO createInfo){
        try {
            statement = conn.prepareStatement(SqlStatesment.INSERT_ATTRIBUTE_META_SQL);
            statement.setString(1, createInfo.getTableName());

            List<AttributeDTO> attributeMetadatas = createInfo.getAttributes();
            for( int columnPosition =0 ; columnPosition < createInfo.getColumnCount() ; columnPosition++ ){
                statement.setString(2, attributeMetadatas.get(columnPosition).getAttributeName());
                statement.setInt(3, columnPosition);
                statement.setInt(4, Integer.parseInt(attributeMetadatas.get(columnPosition).getAttributeLength()));
                statement.executeUpdate();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertStaticsMetadata(CreateInfoDTO createInfo){
        PreparedStatement statement;

        try {
            statement = conn.prepareStatement(SqlStatesment.INSERT_STATICS_META_SQL);
            statement.setString(1, createInfo.getTableName());
            statement.setInt(2, calculateRecordSize(createInfo.getAttributes()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private int calculateRecordSize(List<AttributeDTO> attributeMetadatas){
        int recordSize = 0;
        for( AttributeDTO attributeMetadata : attributeMetadatas ){
            recordSize+=Integer.parseInt(attributeMetadata.getAttributeLength());
        }
        return  recordSize;
    }

    public RelationMetadataDTO getRelationMetadata(String tableName) {
        try {
            statement = conn.prepareStatement(SqlStatesment.SELECT_RELATION_META_SQL);
            statement.setString(1, tableName);
            ResultSet resultSet = statement.executeQuery();
            if(resultSet.next()){
                return new RelationMetadataDTO(resultSet.getString("Relation_name"),resultSet.getInt("Number_of_attribute"),resultSet.getString("File_location"));
            }else{
                return null;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<AttributeMetadataDTO> getAttributeMetadata(String tableName) {
        try {
            statement = conn.prepareStatement(SqlStatesment.SELECT_ATTRIBUTE_META_SQL);
            statement.setString(1, tableName);
            ResultSet resultSet = statement.executeQuery();
            List<AttributeMetadataDTO> attributeMetadatas = new ArrayList<>();
            while(resultSet.next()){
                attributeMetadatas.add(
                        new AttributeMetadataDTO(
                                resultSet.getString("Attribute_name"),
                                resultSet.getInt("Positional"),
                                resultSet.getInt("Length")
                                ));
            }
            return attributeMetadatas;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public StaticMetaDTO getStaticMetadata(String tableName) {
        try {
            statement = conn.prepareStatement(SqlStatesment.SELECT_STATICS_META_SQL);
            statement.setString(1, tableName);
            ResultSet resultSet = statement.executeQuery();
            if(resultSet.next()){
                return new StaticMetaDTO(resultSet.getInt("Record_size"),resultSet.getInt("Record_count"));
            }else{
                return null;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void increaseRecordSize(String tableName) {
        PreparedStatement statement;

        try {
            statement = conn.prepareStatement(SqlStatesment.INCREASE_RECORDSIZE_SQL);
            statement.setString(1, tableName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void decreaseRecordSize(String tableName) {
        PreparedStatement statement;

        try {
            statement = conn.prepareStatement(SqlStatesment.DECREASE_RECORDSIZE_SQL);
            statement.setString(1, tableName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // 메타 데이터용 DB 연결 닫기
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
