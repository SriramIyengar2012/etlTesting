package migrationtest;

import java.sql.ResultSet;
import java.sql.Statement;

import static utils.Utils.*;

public class TargetDatabaseQuery {

    private static ResultSet resultSet;

    public static void countTableValues(String query){
        try {
            Statement stmt= DatabaseConnection.targetConnection().createStatement();
            resultSet = stmt.executeQuery(query);
            resultSetToList(resultSet);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    public static void getColumnSize(String table){
        try {
            Statement stmt= DatabaseConnection.targetConnection().createStatement();
            resultSet = stmt.executeQuery("select * from"+table);
            resultSetToMapColumnSize(resultSet);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
