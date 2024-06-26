package databasesystem.sqlquery.sqlparser;

import databasesystem.dto.*;

import java.util.Arrays;
import java.util.List;

public class SqlParser {

    public static CreateInfoDTO parsingCreateSQL(String ColumnInfos, String tableName) {
        CreateInfoDTO createInfoDTO = new CreateInfoDTO(tableName);
        String[] columns = ColumnInfos.split(",");

        for (String column : columns) {
            // 공백을 기준으로 문자열을 나눔
            String[] parts = column.trim().split(" ");
            AttributeDTO attributeDTO = new AttributeDTO(parts[0], parts[1].substring(5, parts[1].length() - 1) );
            createInfoDTO.addAttribute(attributeDTO);
            //System.out.println(parts[0] + " " + parts[1].substring(5, parts[1].length() - 1)); // 디버깅 확인용
        }

        return createInfoDTO;
    }

    public static InsertInfoDTO parsingInsertSQL(String inputValues, String tableName) {
        List<String> attributeValues = Arrays.stream(inputValues.split(","))
                .map(String::trim)
                .toList();

        //System.out.println(attributeValues); //디버깅확인용
        return new InsertInfoDTO(tableName, attributeValues);
    }

    public static DeleteInfoDTO parsingDeleteSQL(String whereStatement, String tableName) {
        String[] attributeInfo = whereStatement.split("=");
        //System.out.println(tableName + attributeInfo[0] + attributeInfo[1]); //디버깅 확인용
        return new DeleteInfoDTO(tableName, attributeInfo[0], attributeInfo[1]);
    }

    public static SelectInfoDTO parsingSelectSQL(String wantedAttribute, String whereStatement, String tableName) {
        List<String> findAttributes = Arrays.stream(wantedAttribute.split(","))
                .map(String::trim)
                .toList();

        String[] attributeInfo;
        if(whereStatement.equalsIgnoreCase("True")){
            attributeInfo = new String[]{"All", "True"};
        } else {
            attributeInfo = whereStatement.split("=");
        }

        //System.out.println(tableName + attributeInfo[0] + attributeInfo[1]); //디버깅 확인용
        return new SelectInfoDTO(tableName, attributeInfo[0], attributeInfo[1], findAttributes);
    }

        public static JoinInfoDTO parsingJoinSQL(String tableName, String anotherTableName, String joinCondition) {
            // = 기준으로 조인 조건을 분리
            String[] conditions = joinCondition.split("=");

            // 공백 제거
            String leftPart = conditions[0].trim();
            String rightPart = conditions[1].trim();

            // . 을 기준으로 Table과 Column 분리
            String[] leftParts = leftPart.split("\\.");
            String leftTable = leftParts[0].trim();
            String leftColumn = leftParts[1].trim();

            String[] rightParts = rightPart.split("\\.");
            String rightTable = rightParts[0].trim();
            String rightColumn = rightParts[1].trim();

            //System.out.println(tableName + anotherTableName + leftTable + leftColumn + rightTable + rightColumn); //디버깅 확인용

            // 정보를 DTO에 매핑
            if (tableName.equals(leftTable) && anotherTableName.equals(rightTable)) {
                return new JoinInfoDTO(tableName, anotherTableName, leftColumn, rightColumn);
            }else if (tableName.equals(rightTable) && anotherTableName.equals(leftTable)) {
                return new JoinInfoDTO(tableName, anotherTableName, rightColumn, leftColumn);
            }else{ // 잘못된 Sql 구문 입력 (error)
                return null;
            }

        }

}

