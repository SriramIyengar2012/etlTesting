package utils;

import migrationtest.SourceDatabaseQuery;
import migrationtest.TargetDatabaseQuery;

import java.util.List;

public class ComparisonUtils {

    public static void verifyTableCount(String sourceQuery, String targetQuery) {
        try {
            SourceDatabaseQuery.compareFieldCount(sourceQuery);
            TargetDatabaseQuery.countTableValues(targetQuery);
            List<String> rs = Utils.getResultSetValues();
            if(rs.size() > 1) {
                for (int i = 0; i < rs.size(); i++) {


                }
            }
            else {
                AssertionUtils.assertEquals(String.valueOf(Utils.getCount()),String.valueOf(Integer.parseInt(rs.get(0))));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void verifyColumnSize(String sourceTable, String targetTable) {
        try {
            SourceDatabaseQuery.getColumnSizeSource(sourceTable);
            TargetDatabaseQuery.getColumnSize(targetTable);
            AssertionUtils.assertTrue(Utils.getResultSetColumnSize(), Utils.getResultSetColumnSizeSource());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
