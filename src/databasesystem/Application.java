package databasesystem;

import databasesystem.databaseservice.DatabaseService;

import java.util.Scanner;

public class Application {
    Scanner scanner = new Scanner(System.in);
    DatabaseService databaseService;

    final static String MENU = """
        **** 기능 목록 ****
        1. 관계 테이블 생성(create table)
        2. 튜플 삽입(insert record)
        3. 튜플 삭제(delete tuple)
        4. 단일 테이블 튜플 검색(select tuple - one Table)
        5. 조인 연산 수행(Hash Join - Two table)
        6. 프로그램 종료(exit program)
        """;

    public static void main(String[] args) {
        Application application = new Application();
        application.run();
    }

    Application(){
        this.databaseService = new DatabaseService();
    }

    public void run(){
        boolean choiceRepeat = true;
        while(choiceRepeat) {
            System.out.println(MENU);
            System.out.print("기능 선택:");
            int select_num=scanner.nextInt(); // 기능 선택
            scanner.nextLine(); // 버퍼 비우기
            System.out.println();

            switch(select_num) {
                case 1: // 튜플 생성
                    databaseService.createTable();
                    break;
                case 2: // 튜플 삽입
                    databaseService.insertTuple();
                    break;
                case 3: // 튜플 삭제
                    databaseService.deleteTuple();
                    break;
                case 4: // 튜플 검색
                    databaseService.selectTuple();
                    break;
                case 5: // Hash Join 수행
                    databaseService.joinTuple();
                    break;
                case 6:
                    choiceRepeat = false;
                    System.out.println("프로그램을 종료합니다.");
                    databaseService.close();
                    scanner.close();
                    break;
                default:
                    System.out.println(" 잘못된 형식의 입력입니다. 기능을 다시 선택해주세요.");
            }
        }
    }
}