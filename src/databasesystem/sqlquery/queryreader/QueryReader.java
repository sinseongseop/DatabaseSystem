package databasesystem.sqlquery.queryreader;

import databasesystem.dto.*;
import databasesystem.sqlquery.sqlparser.SqlParser;

import java.util.Scanner;

public class QueryReader {
    Scanner scanner = new Scanner(System.in);

    // Create SQL 문을 입력받고 파싱한 후 createInfoDTO를 반환하는 함수
    public CreateInfoDTO createSQL() {
        System.out.print("create table ");
        String tableName = scanner.nextLine().trim();

        System.out.println("생성하기 원하는 컬럼들을 입력하세요.( 컬러명 char(n) 형식 입력, 여러 컬럼 입력 희망시 , 로 구분)");
        String ColumnInfos = scanner.nextLine().trim();
        System.out.println();

        return SqlParser.parsingCreateSQL(ColumnInfos, tableName);
    }

    //Insert SQL 문을 입력받고 파싱 후 insertInfoDTO를 반환하는 함수
    public InsertInfoDTO insertSQL() {
        System.out.print("insert into ");
        String tableName = scanner.nextLine().trim();

        System.out.print("values ");
        String inputValues = scanner.nextLine().trim();
        System.out.println();

        return SqlParser.parsingInsertSQL(inputValues, tableName);
    }

    //Delete SQL 문을 입력받고 파싱 후 DeleteInfoDTO를 반환하는 함수
    public DeleteInfoDTO deleteSQL() {
        System.out.print("delete from ");
        String tableName = scanner.nextLine().trim();

        System.out.print("where ");
        String whereStatement = scanner.nextLine().trim();
        System.out.println();

        return SqlParser.parsingDeleteSQL(whereStatement, tableName);
    }

    //Select SQL 문을 입력받고 파싱 후 SelectInfoDTO를 반환하는 함수
    public SelectInfoDTO selectSQL() {

        System.out.print("select ");
        String wantedAttribute = scanner.nextLine().trim();

        System.out.print("from ");
        String tableName = scanner.nextLine().trim();

        System.out.print("where ");
        String whereStatement = scanner.nextLine().trim();
        System.out.println();

        return SqlParser.parsingSelectSQL(wantedAttribute, whereStatement, tableName);
    }

    //Join SQL 문을 입력 받고 파싱 후 JoinInfoDTO를 반환하는 함수
    public JoinInfoDTO joinSQL(){
        System.out.println("select * ");
        System.out.print("from ");
        String tableName = scanner.nextLine().trim();

        System.out.print("inner join ");
        String AnotherTableName = scanner.nextLine().trim();

        System.out.print("on ");
        String joinCondition = scanner.nextLine().trim();
        System.out.println();

        return SqlParser.parsingJoinSQL(tableName,AnotherTableName,joinCondition);
    }

}
