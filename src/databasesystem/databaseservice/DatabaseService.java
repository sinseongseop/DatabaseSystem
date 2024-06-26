package databasesystem.databaseservice;

import databasesystem.dto.*;
import databasesystem.metadatadb.MetaDataProcessor;
import databasesystem.sqlquery.queryreader.QueryReader;
import databasesystem.systemdb.FileProcessor;

import java.util.List;

public class DatabaseService {

    private final QueryReader sqlReader;
    private final FileProcessor fileProcessor;
    private final MetaDataProcessor metaDataProcessor;

    public DatabaseService(){
        this.sqlReader = new QueryReader();
        this.fileProcessor = new FileProcessor();
        this.metaDataProcessor = new MetaDataProcessor();
    }

    public void createTable() {
        CreateInfoDTO createInfo = sqlReader.createSQL();
        metaDataProcessor.createMetadata(createInfo);
        fileProcessor.createfile(createInfo);
    }

    public void insertTuple() {
        InsertInfoDTO insertInfo = sqlReader.insertSQL();
        Object[] metadatas = getMetaDatas(insertInfo.getTableName());
        if( metadatas != null){
            fileProcessor.insertTuple(insertInfo, (RelationMetadataDTO)metadatas[0], (List<AttributeMetadataDTO>)metadatas[1],(StaticMetaDTO)metadatas[2]);
            metaDataProcessor.increaseRecordSize(insertInfo.getTableName());
        }
    }

    public void deleteTuple() {
        DeleteInfoDTO deleteInfo = sqlReader.deleteSQL();
        Object[] metadatas = getMetaDatas(deleteInfo.getTableName());
        if( metadatas != null){
            fileProcessor.deleteTuple(deleteInfo,(RelationMetadataDTO)metadatas[0], (List<AttributeMetadataDTO>)metadatas[1],(StaticMetaDTO)metadatas[2]);
            metaDataProcessor.decreaseRecordSize(deleteInfo.getTableName());
        }

    }

    public void selectTuple() {
        SelectInfoDTO selectInfo = sqlReader.selectSQL();
        Object[] metadatas = getMetaDatas(selectInfo.getTableName());
        if( metadatas != null){
            fileProcessor.selectTuple(selectInfo,(RelationMetadataDTO)metadatas[0], (List<AttributeMetadataDTO>)metadatas[1],(StaticMetaDTO)metadatas[2]);
        }
    }

    public void joinTuple(){
        JoinInfoDTO joinInfoDTO = sqlReader.joinSQL();
        if(joinInfoDTO == null){
            System.out.println("SQL 구문을 잘못 입력하였습니다. ");
            System.out.println();
            return;
        }

        Object[] oneMetadatas = getMetaDatas(joinInfoDTO.getTableName());
        Object[] anotherMetadatas = getMetaDatas(joinInfoDTO.getAnotherTableName());

        if(oneMetadatas != null && anotherMetadatas != null){
            fileProcessor.joinTuple(joinInfoDTO,(RelationMetadataDTO)oneMetadatas[0], (List<AttributeMetadataDTO>)oneMetadatas[1],(StaticMetaDTO)oneMetadatas[2],
                    (RelationMetadataDTO)anotherMetadatas[0], (List<AttributeMetadataDTO>)anotherMetadatas[1],(StaticMetaDTO)anotherMetadatas[2]);

        }
    }

    private Object[] getMetaDatas(String tableName){
        RelationMetadataDTO relationMetadataDTO = metaDataProcessor.getRelationMetadata(tableName);
        if(relationMetadataDTO == null){
            System.out.println(tableName + " 테이블은 생성되어 있지 않습니다.");
            return null;
        }

        List<AttributeMetadataDTO> attributeMetadatas = metaDataProcessor.getAttributeMetadata(tableName);
        StaticMetaDTO staticMetaDTO = metaDataProcessor.getStaticMetadata(tableName);

        return new Object[]{relationMetadataDTO, attributeMetadatas, staticMetaDTO};
    }

    public void close() {
        metaDataProcessor.close();
    }
}
