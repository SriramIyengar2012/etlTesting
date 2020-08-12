package utils;

import migrationtest.SourceDatabaseQuery;
import migrationtest.TargetDatabaseQuery;

public class ComparisonUtils {

    public static void verifyTableCount(String sourceQuery, String targetQuery) {
        try {
            SourceDatabaseQuery.compareFieldCount(sourceQuery);
            TargetDatabaseQuery.countTableValues(targetQuery);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
