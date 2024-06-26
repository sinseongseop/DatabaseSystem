package databasesystem.metadatadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MetaDataDB {

    Connection conn ;

    public Connection dbConnection() {
        open_db();
        return conn;
    }

    public void closeDB() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void open_db() { //DB 열기 함수
        try {
            conn= DriverManager.getConnection(ConnectionInfo.DB_URL, ConnectionInfo.USER, ConnectionInfo.PASS);
        }catch(Exception e){
            System.out.println(" 메타 데이터용 DB 열기 실패. Exception:" + e);
            throw new RuntimeException();
        }
    }

}
