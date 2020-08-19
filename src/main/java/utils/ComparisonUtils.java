package utils;

import migrationtest.SourceDatabaseQuery;
import migrationtest.TargetDatabaseQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ComparisonUtils {

    public static void verifyTableCount(String sourceQuery, String targetQuery) {
        try {
            SourceDatabaseQuery.dataProfileTest(sourceQuery);
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

    public static void verifyColumnSum(String sourceColumn, String targetColumn, String sourceTable, String targetTable){
        SourceDatabaseQuery.schemaVerifyMultiColumns("select " + sourceColumn + " " + "from " + sourceTable, sourceColumn);
        Iterator schemaIterator = Utils.getSchemaNames().entrySet().iterator();
        while (schemaIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry) schemaIterator.next();
            String value = (String) mapElement.getValue();
            if (value.equalsIgnoreCase("double")) {
                SourceDatabaseQuery.sumOfColumnswithDoubles("select SUM(" + mapElement.getKey() + ") from " + sourceTable);
            }
            if (value.equalsIgnoreCase("Long")) {
                SourceDatabaseQuery.sumOfColumnswithLongs("select SUM(" + mapElement.getKey() + ") from " + sourceTable);
            }
        }
    }

    public static void verifyData(String sourceColumn, String targetColumn, String sourceTable, String targetTable) {
        SourceDatabaseQuery.multiColumnsSum("Select "+ sourceColumn + " " +"from "+ sourceTable,sourceColumn);
    }

}
