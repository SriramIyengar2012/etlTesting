package Implementation.database;

import com.thoughtworks.gauge.*;
import utils.ComparisonUtils;

public class DatabaseImplementation {

    @Step("Verify Count of Column between <Source> and <Target>")
    public void countOfColumnsVerify(String sourceQuery, String targetQuery) {
        ComparisonUtils.verifyTableCount(sourceQuery, targetQuery);

    }

    @Step("Verify if Column Size of <SourceTable> table in Source is matching with <TargetTable> table in Target")
    public void verifyColumnLength(String sourceTable, String targetTable){
        ComparisonUtils.verifyColumnSize(sourceTable,targetTable);

    }

    @Step("Verify Sum of Columns between <SourceColumn> column from <table1> in Source and <TargetColumn> column from <table2> in Target")
    public void verifyColumnSum(String sourceColumn, String targetColumn, String sourceTable, String targetTable) {
      ComparisonUtils.verifyColumnSum(sourceColumn,sourceTable,targetColumn,targetTable);
    }

    @Step("Verify if Data of columns <sourceColumns> from <sourceTable> in Source is same as <targetColumns> from <targetTable> in Target")
    public void verifyDatainColumns(String sourceColumn, String targetColumn, String sourceTable, String targetTable){
        ComparisonUtils.verifyData(sourceColumn,sourceTable,targetColumn,targetTable);
    }

    @Step("verify if <targetColumn> from <targetTable> in target is not nullable")
    public void verifyNullCheck(String targetColumn, String targetTable){



    }




}
