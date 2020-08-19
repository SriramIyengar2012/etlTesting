package utils;



import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
;

public class Utils {

    static List<String> values = new ArrayList<>();
    static HashMap<String, String> schemaNames = new HashMap();
    static HashMap<String, String> nullValues = new HashMap();
    static List<String> resultSetValues = new ArrayList<>();
    static HashMap<String, String> resultSetColumnSize = new HashMap();
    static HashMap<String, String> multiColumnData = new HashMap();
    static HashMap<String, String> resultSetColumnSizeSource  = new HashMap();
    static int count;

    public static double getSumCol() {
        return sumCol;
    }

    public static void setSumCol(double sumCol) {
        Utils.sumCol = sumCol;
    }

    static double sumCol;

    public static void addValues(String val){
        values.add(val);
    }

    public static List<String> getValues () {
        return values;
    }



    public static HashMap<String,String> getSchemaNames () {
        return schemaNames;
    }

    public static HashMap<String,String> getMultiColumnData () {
        return multiColumnData;
    }


    public static HashMap<String,String>getNullValues () {
        return nullValues;
    }

    public static void setCount(int c){
        count = c;
    }

    public static int getCount(){
        return count;
    }

    public static void resultSetToList(ResultSet rs){
       try {
           while (rs.next()) {
               ResultSetMetaData rsmd = rs.getMetaData();
               for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                   if (rs.getMetaData().getColumnType(1) == Types.INTEGER) {
                       resultSetValues.add(String.valueOf(rs.getInt(i)));
                   }
                   if (rs.getMetaData().getColumnType(1) == Types.VARCHAR) {
                       resultSetValues.add(String.valueOf(rs.getString(i)));
                   }
                   if (rs.getMetaData().getColumnType(1) == Types.DOUBLE) {
                       resultSetValues.add(String.valueOf(rs.getDouble(i)));
                   }
               }

           }
       }
       catch(Exception e) {
           e.printStackTrace();
       }

    }

    public static void resultSetToMapColumnSize(ResultSet rs){
        try {
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    resultSetColumnSize.put(rsmd.getColumnName(i), String.valueOf(rsmd.getColumnDisplaySize(i)));
                }

            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }


    public static void resultSetToMapColumnSizeSource(ResultSet rs){
        try {
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    resultSetColumnSizeSource.put(rsmd.getColumnName(i), String.valueOf(rsmd.getColumnDisplaySize(i)));
                }

            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

    public static List<String> getResultSetValues () {
        return resultSetValues;
    }

    public static HashMap<String, String> getResultSetColumnSize () {
        return resultSetColumnSize;
    }

    public static HashMap<String, String> getResultSetColumnSizeSource() {
        return resultSetColumnSizeSource;
    }

}
