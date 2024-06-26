package databasesystem.systemdb;

import databasesystem.dto.*;
import databasesystem.hash.Bucket;
import databasesystem.hash.HashFunction;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FileProcessor {

    private final int BLOCK_SIZE = 100; // 블록 input/output 단위 크기
    private final int PARTIAL_COUNT = 4; // Join 연산 수행시 릴레이션의 분할 개수
    private final int HASH_BUCKET_COUNT = 3; //hash bucket의 개수

    public void createfile(CreateInfoDTO createInfo) {
        String filePath = createInfo.getTableName() + ".txt";
        try {
            // BufferedOutputStream을 이용해 블록 단위 입출력 수행.
            FileOutputStream fileOutputStream = new FileOutputStream(filePath);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);

            // 파일 헤더 생성
            String fileHeader = "L$pos -1";
            String blankSpace = generateBlank(BLOCK_SIZE - fileHeader.getBytes().length);

            // 문자열을 바이트 배열로 변환하여 블록단위로 파일에 쓰기
            byte[] bytes = (fileHeader + blankSpace).getBytes();
            //System.out.println(bytes.length); //디버깅용 코드

            bufferedOutputStream.write(bytes);
            bufferedOutputStream.close();

            System.out.println(createInfo.getTableName() + " 테이블이 생성되었습니다.\n");
        } catch (IOException e) {
            System.out.println(" 파일 생성 오류 발생 : " + e.getMessage());
        }
    }

    private static String generateBlank(int blankLenghth) {
        return " ".repeat(Math.max(0, blankLenghth));
    }

    public void insertTuple(InsertInfoDTO insertInfo, RelationMetadataDTO relationMetadataDTO, List<AttributeMetadataDTO> attributeMetadatas, StaticMetaDTO staticMetaDTO) {
        String filePath = relationMetadataDTO.getFileLocation();
        String headerBlockData = readBlock(filePath, 0L);
        String header = headerBlockData.substring(0, staticMetaDTO.getRecordSize());
        int recordSize = staticMetaDTO.getRecordSize();
        if (header.contains(" -1 ")) {
            // header의 값이 -1"면 파일의 끝에 튜플을 삽입
            insertRecordInEOF(insertInfo, relationMetadataDTO, attributeMetadatas, recordSize);

        } else {
            // "L$pos -1" 문자열이 포함되어 있지 않으면 pos 뒤에 있는 숫자의 바이트 위치에 튜플을 삽입
            insertRecordInMiddle(insertInfo, relationMetadataDTO, attributeMetadatas, header, recordSize, headerBlockData);
        }

    }

    private void insertRecordInMiddle(InsertInfoDTO insertInfo, RelationMetadataDTO relationMetadataDTO, List<AttributeMetadataDTO> attributeMetadatas, String header, int recordSize, String headerBlockData) {
        String filePath = relationMetadataDTO.getFileLocation();
        int posIndex = header.substring(0, recordSize).indexOf("pos");

        if (posIndex != -1) {
            int position = Integer.parseInt(header.substring(posIndex + 4).trim());

            int offset = position%100;
            int insertPosition = position-offset;
            String BlockData = readBlock(filePath, insertPosition);
            String nextPositionData = BlockData.substring(offset, offset+ recordSize);

            //header 블록의 header 업데이트
            String headerBlock = nextPositionData + headerBlockData.substring(recordSize);
            writeBlock(filePath,headerBlock,0);

            //insert 위치에 블록 쓰기
            String writingData = BlockData.substring(0,offset)
                                +writeTuple(insertInfo, attributeMetadatas, relationMetadataDTO.getAttributeCnt())
                                +BlockData.substring(offset+ recordSize);

            //header와 writingData가 같은 블록인 경우 예외 처리
            if(position < BLOCK_SIZE){
                writingData = headerBlock.substring(0, recordSize)+writingData.substring(recordSize);
            }

            writeBlock(filePath,writingData,insertPosition);
        }
    }

    private void insertRecordInEOF(InsertInfoDTO insertInfo, RelationMetadataDTO relationMetadataDTO, List<AttributeMetadataDTO> attributeMetadatas, int recordSize) {
        String filePath = relationMetadataDTO.getFileLocation();
        Long EOFPosition = getEndOfFilePosition(filePath);
        String readBlockData = readBlock(filePath, EOFPosition - BLOCK_SIZE);
        int offset = 0;
        long insertPosition = EOFPosition;
        StringBuilder writingData = new StringBuilder();

        while (offset <= BLOCK_SIZE - recordSize) {
            if (readBlockData.substring(offset, offset + recordSize).trim().isEmpty()) {
                writingData.append(writeTuple(insertInfo, attributeMetadatas, relationMetadataDTO.getAttributeCnt()))
                            .append(generateBlank(BLOCK_SIZE - (offset + recordSize)));
                insertPosition = EOFPosition - BLOCK_SIZE;
                break;
            } else {
                writingData.append(readBlockData, offset, offset + recordSize);
                offset += recordSize;
            }
        }

        if (offset+ recordSize > BLOCK_SIZE) {
            writingData = new StringBuilder(writeTuple(insertInfo, attributeMetadatas, relationMetadataDTO.getAttributeCnt())
                    + generateBlank(BLOCK_SIZE - recordSize));
        }

        writeBlock(filePath, writingData.toString(),insertPosition);
    }

    public void deleteTuple(DeleteInfoDTO deleteInfo, RelationMetadataDTO relationMetadataDTO, List<AttributeMetadataDTO> attributeMetadatas, StaticMetaDTO staticMetaDTO) {
        String filePath = relationMetadataDTO.getFileLocation();
        Long EOFPosition = getEndOfFilePosition(filePath);
        int blockNum=0;
        String searchAttribute = deleteInfo.getSearchAttribute();
        String searchValue = deleteInfo.getSearchValue();
        while((long) blockNum *BLOCK_SIZE < EOFPosition){
            String readingData =readBlock(filePath, (long) blockNum *BLOCK_SIZE);
            List<Integer> findTupleOffset = findTuplesPostion(readingData,searchAttribute,searchValue,attributeMetadatas,staticMetaDTO);
            if( !findTupleOffset.isEmpty()){
                deleteInFile(readingData,findTupleOffset, staticMetaDTO,blockNum, filePath);
            }
            blockNum+=1;
        }

    }

    public void selectTuple(SelectInfoDTO selectInfo, RelationMetadataDTO relationMetadataDTO, List<AttributeMetadataDTO> attributeMetadatas, StaticMetaDTO staticMetaDTO) {
        String filePath = relationMetadataDTO.getFileLocation();
        Long EOFPosition = getEndOfFilePosition(filePath);
        int blockNum=0;
        String searchAttribute = selectInfo.getSearchAttribute();
        String searchValue = selectInfo.getSearchValue();
        printAttributes(selectInfo,attributeMetadatas);

        while((long) blockNum *BLOCK_SIZE < EOFPosition){
            String readingData =readBlock(filePath, (long) blockNum *BLOCK_SIZE);
            List<Integer> findTupleOffset = findTuplesPostion(readingData,searchAttribute,searchValue,attributeMetadatas,staticMetaDTO);
            printResultSet(readingData, selectInfo, findTupleOffset, attributeMetadatas);
            blockNum+=1;
        }
        System.out.println();
    }

    public void joinTuple(JoinInfoDTO joinInfoDTO, RelationMetadataDTO onerelationMetadataDTO, List<AttributeMetadataDTO> oneAttributeMetadatas, StaticMetaDTO oneStaticMetaDTO,
                          RelationMetadataDTO anotherrelationMetadataDTO, List<AttributeMetadataDTO> anotherAttributeMetadatas, StaticMetaDTO anotherStaticMetaDTO) {
        // Partition 수행
        List<String> oneTempFiles = makePartitionFiles(joinInfoDTO.getTableJoinColumn(), onerelationMetadataDTO, oneAttributeMetadatas, oneStaticMetaDTO);
        List<String> anotherTempFiles = makePartitionFiles(joinInfoDTO.getAnotherTableJoinColumn(), anotherrelationMetadataDTO, anotherAttributeMetadatas, anotherStaticMetaDTO);

        // 모든 컬럼명 출력
        printAllColumnName(oneAttributeMetadatas, anotherAttributeMetadatas);

        // 각 partition 마다 join 수행
        for (int i = 0; i < PARTIAL_COUNT; i++) {
            // Hash Index 초기화
            List<List<Bucket>> bucketIndex = initializeBucketIndex(HASH_BUCKET_COUNT);

            // Ti 임시파일을 읽고 데이터를 메모리로 로드하기
            String anotherFilePath = anotherTempFiles.get(i);
            List<String> memoryBuffer = loadFileToMemory(anotherFilePath);

            // 인덱스 구축을 위해 필요한 메타데이터 검색
            int anotherRecordSize = anotherStaticMetaDTO.getRecordSize();
            int[] anotherColumnInfo = getColumnPositionAndSize(anotherAttributeMetadatas, joinInfoDTO.getAnotherTableJoinColumn());

            // 출력을 위한 메타데이터 검색
            List<Integer> oneColumnPositions = getColumnPositions(oneAttributeMetadatas);
            List<Integer> anotherColumnPositions = getColumnPositions(anotherAttributeMetadatas);

            // HashIndex 구축
            buildHashIndex(bucketIndex, memoryBuffer, anotherColumnInfo, anotherRecordSize);

            // 조인 수행
            performJoin(oneTempFiles.get(i), bucketIndex, memoryBuffer, oneStaticMetaDTO,
                    oneAttributeMetadatas, joinInfoDTO.getTableJoinColumn(), oneColumnPositions, anotherColumnPositions);
        }

        System.out.println();
    }


    //특정 Table을 Partition 해주는 함수
    public List<String> makePartitionFiles(String partialColumn, RelationMetadataDTO relationMetadataDTO,
                                           List<AttributeMetadataDTO> attributeMetadatas, StaticMetaDTO staticMetaDTO) {

        // 원본 파일을 읽기 위함 변수들 초기화
        String originFilePath = relationMetadataDTO.getFileLocation();
        long originEOFPosition = getEndOfFilePosition(originFilePath);
        int originBlockNum = 0;

        //필요한 메타데이터 정보 초기화
        int recordSize = staticMetaDTO.getRecordSize();
        int[] columnInfo = getColumnPositionAndSize(attributeMetadatas,partialColumn);
        int columnPosition = columnInfo[0];
        int columnSize = columnInfo[1];

        // 임시 파일에 쓰기위한 변수들 초기화
        List<String> temporaryFiles = initializeTemporaryFiles(originFilePath);
        List<String> tempMemoryBuffers = initializeTempMemoryBuffers();
        List<Integer> tempOffsets = initializeTempOffsets();
        List<Integer> tempBlockNums = initializeTempBlockNums();

        //Partition 수행
        while ((long) originBlockNum * BLOCK_SIZE < originEOFPosition) {
            String readingData = readBlock(originFilePath, (long) originBlockNum * BLOCK_SIZE);
            partitionBlock(recordSize, columnPosition, columnSize, temporaryFiles,
                           tempMemoryBuffers, tempOffsets, tempBlockNums, readingData);
            originBlockNum++;
        }

        //버퍼에 남아있는 데이터를 Disk에 쓰기
        writeRemainingData(temporaryFiles, tempMemoryBuffers, tempOffsets, tempBlockNums);

        // 임시 파일명들 반환
        return temporaryFiles;
    }

    // 임시파일명 초기화 함수
    private List<String> initializeTemporaryFiles(String originFilePath) {
        List<String> temporaryFiles = new ArrayList<>();
        for (int i = 0; i < PARTIAL_COUNT; i++) {
            temporaryFiles.add("T" + i + originFilePath); // ex) T0book.txt
            //System.out.println(temporaryFiles.get(i)); //디버깅 확인용
        }
        return temporaryFiles;
    }

    // 임시 메모리 버퍼 초기화 함수
    private List<String> initializeTempMemoryBuffers() {
        List<String> tempMemoryBuffers = new ArrayList<>();
        for (int i = 0; i < PARTIAL_COUNT; i++) {
            tempMemoryBuffers.add("");
        }
        return tempMemoryBuffers;
    }

    //임시 메모리Offset 초기화 함수
    private List<Integer> initializeTempOffsets() {
        List<Integer> tempOffsets = new ArrayList<>();
        for (int i = 0; i < PARTIAL_COUNT; i++) {
            tempOffsets.add(0);
        }
        return tempOffsets;
    }

    //임시 파일 블록의 개수 초기화 함수
    private List<Integer> initializeTempBlockNums() {
        List<Integer> tempBlockNums = new ArrayList<>();
        for (int i = 0; i < PARTIAL_COUNT; i++) {
            tempBlockNums.add(0);
        }
        return tempBlockNums;
    }

    //한블록을 partition 수행하는 함수
    private void partitionBlock(int recordSize, int columnPosition, int columnSize, List<String> temporaryFile,
                                List<String> tempMemoryBuffers, List<Integer> tempOffsets, List<Integer> tempBlockNums, String readingData) {
        int recordOffset = 0;
        while (recordOffset <= BLOCK_SIZE - recordSize) {
            String record = readingData.substring(recordOffset, recordOffset + recordSize);
            if (!record.contains("L$pos") && !record.trim().isEmpty()) {
                String columnValue = record.substring(columnPosition, columnPosition + columnSize);
                int hashValue = HashFunction.partitionHash(columnValue, PARTIAL_COUNT); // ColumnValue를 해쉬함수의 input으로 넣어 partition 할 위치를 파악
                // System.out.println(columnValue+hashValue);  //디버깅 확인용
                saveRecordInPartition(recordSize,temporaryFile, tempMemoryBuffers, tempOffsets, tempBlockNums, hashValue, record);
            }
            recordOffset += recordSize;
        }
    }

    //Partition에 record를 저장하고 버퍼가 다 차면 Disk에 저장하는 함수
    private void saveRecordInPartition(int recordSize, List<String> temporaryFile,
                                       List<String> tempMemoryBuffers, List<Integer> tempOffsets,
                                       List<Integer> tempBlockNums, int hashValue, String record) {

        String bufferValue = tempMemoryBuffers.get(hashValue);
        int bufferOffset = tempOffsets.get(hashValue);
        bufferValue += record;
        bufferOffset += recordSize;

        if (bufferOffset > BLOCK_SIZE - recordSize) {
            writeBlock(temporaryFile.get(hashValue), bufferValue + generateBlank(BLOCK_SIZE - bufferOffset),
                    tempBlockNums.get(hashValue) * BLOCK_SIZE);
            tempMemoryBuffers.set(hashValue, "");
            tempOffsets.set(hashValue, 0);
            tempBlockNums.set(hashValue, tempBlockNums.get(hashValue) + 1);
        } else {
            tempMemoryBuffers.set(hashValue, bufferValue);
            tempOffsets.set(hashValue, bufferOffset);
        }
    }

    //버퍼에 남아 있는 데이터를 쓰는 함수
    private void writeRemainingData(List<String> temporaryFiles, List<String> tempMemoryBuffers,
                                    List<Integer> tempOffsets, List<Integer> tempBlockNums) {
        for (int tempFileNum = 0; tempFileNum < PARTIAL_COUNT; tempFileNum++) {
            if (tempOffsets.get(tempFileNum) > 0) {
                writeBlock(temporaryFiles.get(tempFileNum), tempMemoryBuffers.get(tempFileNum) +
                        generateBlank(BLOCK_SIZE - tempOffsets.get(tempFileNum)), tempBlockNums.get(tempFileNum)*BLOCK_SIZE);
            }
        }
    }

    //BucketIndex를 초기화 하는 함수
    private List<List<Bucket>> initializeBucketIndex(int bucketCount) {
        List<List<Bucket>> bucketIndex = new ArrayList<>(bucketCount);
        for (int j = 0; j < bucketCount; j++) {
            bucketIndex.add(new ArrayList<>());
        }
        return bucketIndex;
    }

    //파일의 내용 전체를 메모리로 로드 하는 함수
    private List<String> loadFileToMemory(String filePath) {
        List<String> memoryBuffer = new ArrayList<>();
        long EOFPosition = getEndOfFilePosition(filePath);
        int blockNum = 0;

        while ((long) blockNum * BLOCK_SIZE < EOFPosition) {
            memoryBuffer.add(readBlock(filePath, (long) blockNum * BLOCK_SIZE));
            blockNum++;
        }
        return memoryBuffer;
    }

    //찾는 컬럼의 위치와 최대 길이를 가져오는 함수
    private int[] getColumnPositionAndSize(List<AttributeMetadataDTO> attributeMetadatas, String targetColumn) {
        int position =0;
        for (AttributeMetadataDTO attributeMetadata : attributeMetadatas) {
            if (attributeMetadata.getAttributeName().equals(targetColumn)) {
                return new int[]{position, attributeMetadata.getMaxLength()};
            }
            position+=attributeMetadata.getMaxLength();
        }
        return new int[]{0, 0}; // 기본값
    }

    //해쉬 인덱스 구축
    private void buildHashIndex(List<List<Bucket>> bucketIndex, List<String> memoryBuffer, int[] columnInfo, int recordSize) {
        int columnPosition = columnInfo[0];
        int columnSize = columnInfo[1];

        for (int blockNum = 0; blockNum < memoryBuffer.size() ; blockNum++) {
            String readingData = memoryBuffer.get(blockNum);
            int recordOffset = 0;
            while (recordOffset <= BLOCK_SIZE - recordSize) {
                String record = readingData.substring(recordOffset, recordOffset + recordSize);
                if (!record.trim().isEmpty()) {
                    String columnValue = record.substring(columnPosition, columnPosition + columnSize);
                    int hashValue = HashFunction.indexHash(columnValue, HASH_BUCKET_COUNT); // ColumnValue를 해시 함수의 입력으로 넣어 해시 값 얻기
                    bucketIndex.get(hashValue).add(new Bucket(columnValue, blockNum, recordOffset, recordSize));
                }
                recordOffset += recordSize;
            }
        }
    }

    // Hash Index를 이용하여 Join을 수행하는 함수
    private void performJoin(String filePath, List<List<Bucket>> bucketIndex, List<String> memoryBuffer, StaticMetaDTO staticMetaDTO,
                             List<AttributeMetadataDTO> attributeMetadatas, String joinColumn, List<Integer> oneColumnPositions, List<Integer> anotherColumnPositions) {
        long EOFPosition = getEndOfFilePosition(filePath);
        int recordSize = staticMetaDTO.getRecordSize();
        int[] columnInfo = getColumnPositionAndSize(attributeMetadatas, joinColumn);

        int columnPosition = columnInfo[0];
        int columnSize = columnInfo[1];

        int blockNum = 0;
        while ((long) blockNum * BLOCK_SIZE < EOFPosition) {
            String readingData = readBlock(filePath, (long) blockNum * BLOCK_SIZE);
            int recordOffset = 0;
            while (recordOffset <= BLOCK_SIZE - recordSize) {
                String record = readingData.substring(recordOffset, recordOffset + recordSize);
                if (!record.trim().isEmpty()) {
                    String columnValue = record.substring(columnPosition, columnPosition + columnSize);
                    int hashValue = HashFunction.indexHash(columnValue, HASH_BUCKET_COUNT); // ColumnValue를 해시 함수의 입력으로 넣어 해시 값 얻기

                    for (Bucket bucket : bucketIndex.get(hashValue)) {
                        if (bucket.getKey().equals(columnValue)) {
                            printRecord(record, oneColumnPositions);
                            String anotherRecord = memoryBuffer.get(bucket.getBlockNum()).substring(bucket.getOffset(), bucket.getOffset() + bucket.getRecordLength());
                            printRecord(anotherRecord, anotherColumnPositions);
                            System.out.println();
                        }
                    }
                }
                recordOffset += recordSize;
            }
            blockNum++;
        }
    }

    // 모든 컬럼이름 출력
    private void printAllColumnName(List<AttributeMetadataDTO> oneAttributeMetadatas, List<AttributeMetadataDTO> anotherAttributeMetadatas) {
        for(AttributeMetadataDTO attributeMetadata : oneAttributeMetadatas){
            System.out.print(attributeMetadata.getAttributeName()+generateBlank(20-attributeMetadata.getAttributeName().length()));
        }
        for(AttributeMetadataDTO attributeMetadata : anotherAttributeMetadatas){
            System.out.print(attributeMetadata.getAttributeName()+generateBlank(20-attributeMetadata.getAttributeName().length()));
        }
        System.out.println();
    }

    //모든 컬럼의 메타데이터를 얻는 함수
    private List<Integer> getColumnPositions(List<AttributeMetadataDTO> attributeMetadatas) {
        List<Integer> columnPositions = new ArrayList<>();
        int position = 0;
        columnPositions.add(position);
        for (AttributeMetadataDTO attributeMetadata : attributeMetadatas) {
            position += attributeMetadata.getMaxLength();
            columnPositions.add(position);
        }
        return columnPositions;
    }

    public Long getEndOfFilePosition(String filePath) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            return randomAccessFile.length();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String writeTuple(InsertInfoDTO insertInfo, List<AttributeMetadataDTO> attributeMetadata, int columnCnt) {
        StringBuilder record = new StringBuilder();
        List<String> values = insertInfo.getValues();

        for (int i = 0; i < columnCnt; i++) {
            record.append(values.get(i));
            record.append(generateBlank(attributeMetadata.get(i).getMaxLength() - values.get(i).length()));
        }

        return record.toString();
    }

    public String readBlock(String filePath, long seekPosition) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            randomAccessFile.seek(seekPosition);
            byte[] readBytes = new byte[BLOCK_SIZE];
            randomAccessFile.readFully(readBytes);
            return new String(readBytes, StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void writeBlock(String filePath, String data, long seekPosition) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "rw")) {
            randomAccessFile.seek(seekPosition);
            byte[] writingBytes = data.getBytes(StandardCharsets.UTF_8);
            randomAccessFile.write(writingBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Integer> findTuplesPostion(String blockData,String searchAttribute, String searchValue, List<AttributeMetadataDTO> attributeMetadatas,  StaticMetaDTO staticMeta ){
        List<Integer> findTuplesOffset = new ArrayList<>();

        int searchOffset = getAttributePosition(attributeMetadatas, searchAttribute);
        int searchSize = getAttributeSize(attributeMetadatas,searchAttribute);

        int recordOffset =0 ;
        int recordSize = staticMeta.getRecordSize();

        while( recordOffset + recordSize <= BLOCK_SIZE){
            if((searchAttribute.equalsIgnoreCase("All") || blockData.substring(recordOffset+searchOffset ,recordOffset+searchOffset+searchSize).trim().equals(searchValue.trim()))){
                if(blockData.length()>recordSize
                        && !blockData.substring(recordOffset, recordOffset + recordSize).trim().isEmpty()
                        && !blockData.substring(recordOffset,recordOffset+recordSize).contains("L$pos")) {
                    findTuplesOffset.add(recordOffset);
                }
            }
            recordOffset+= recordSize;
        }

        return findTuplesOffset;
    }

    public void deleteInFile(String readingData, List<Integer> findTupleOffset,StaticMetaDTO staticMeta,int blockNum, String filePath){
        String headerBlockData = readBlock(filePath, 0L);
        String header = headerBlockData.substring(0,staticMeta.getRecordSize());
        String updateBlock="";
        for( int tupleOffset: findTupleOffset ){
                updateBlock=readingData.substring(0,tupleOffset)+header+readingData.substring(tupleOffset+ staticMeta.getRecordSize());
                String nextPointList="L$pos "+(blockNum*BLOCK_SIZE+tupleOffset);
                header=nextPointList+generateBlank(staticMeta.getRecordSize()-nextPointList.length());
                //blockNum 인경우 헤더랑 updateBlock이 동일한 블록이므로 예외 처리
                if(blockNum==0){
                    updateBlock = header+updateBlock.substring(staticMeta.getRecordSize());
                }
        }

        if(!updateBlock.isEmpty()){
            String NewHeaderBlockData = header+headerBlockData.substring(staticMeta.getRecordSize());
            writeBlock(filePath,NewHeaderBlockData,0L);
            writeBlock(filePath, updateBlock , (long) blockNum *BLOCK_SIZE);
        }

    }

    public void printAttributes(SelectInfoDTO selectInfoDTO, List<AttributeMetadataDTO> attributeMetadatas){
        List<String> findAttributes = selectInfoDTO.getFindAttributes();
        if(findAttributes.get(0).equals("*")){
            for(AttributeMetadataDTO attributeMetadata : attributeMetadatas){
                System.out.print(attributeMetadata.getAttributeName()+generateBlank(20-attributeMetadata.getAttributeName().length()));
            }
        }else{
            for(String findAttribute :findAttributes){
                for(AttributeMetadataDTO attributeMetadata : attributeMetadatas){
                    if(attributeMetadata.getAttributeName().equals(findAttribute)){
                        System.out.print(attributeMetadata.getAttributeName()+generateBlank(20-attributeMetadata.getAttributeName().length()));
                        break;
                    }
                }
            }
        }
        System.out.println();
    }

    // 레코드를 규격에 맞게 출력하는 함수
    private void printRecord(String record, List<Integer> columnPositions) {
        for (int i = 0; i < columnPositions.size() - 1; i++) {
            String column = record.substring(columnPositions.get(i), columnPositions.get(i + 1)).trim();
            System.out.print(column + generateBlank(20 - column.length()));
        }
    }


    public void printResultSet(String blockData,SelectInfoDTO selectInfo ,List<Integer> tuplesOffset,List<AttributeMetadataDTO> attributeMetadatas){
        for( int tupleOffset : tuplesOffset){
            if(selectInfo.getFindAttributes().get(0).equals("*")){
                int offset=0;
                for(AttributeMetadataDTO attributeMetada : attributeMetadatas){
                    int attributeLength=attributeMetada.getMaxLength();
                    System.out.print(blockData.substring(tupleOffset+offset,tupleOffset+offset+attributeLength)+generateBlank(20-attributeLength));
                    offset+=attributeLength;
                }
                System.out.println();
            }else{
                for(String findAttribute : selectInfo.getFindAttributes()){
                    int offset=0;
                    for(AttributeMetadataDTO attributeMetada : attributeMetadatas){
                        int attributeLength=attributeMetada.getMaxLength();
                        if(attributeMetada.getAttributeName().equals(findAttribute)){
                            System.out.print(blockData.substring(tupleOffset+offset,tupleOffset+offset+attributeLength)+generateBlank(20-attributeLength));
                        }
                        offset+=attributeLength;
                    }
                }
                System.out.println();
            }
        }
    }

    private static int getAttributePosition(List<AttributeMetadataDTO> attributeMetadatas, String searchAttribute) {
        int attributeOffset =0;
        for(AttributeMetadataDTO attributeMetada : attributeMetadatas){
            if(attributeMetada.getAttributeName().equals(searchAttribute)){
                return attributeOffset;
            }
            attributeOffset +=attributeMetada.getMaxLength();
        }

        return -1;
    }

    private static int getAttributeSize(List<AttributeMetadataDTO> attributeMetadatas, String searchAttribute) {
        for(AttributeMetadataDTO attributeMetada : attributeMetadatas){
            if(attributeMetada.getAttributeName().equals(searchAttribute)){
                return attributeMetada.getMaxLength();
            }
        }

        return -1;
    }

}