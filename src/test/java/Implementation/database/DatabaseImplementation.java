package Implementation.database;

import com.thoughtworks.gauge.*;
import utils.AssertionUtils;
import utils.ComparisonUtils;
import utils.Utils;

import java.util.List;

public class DatabaseImplementation {

    @Step("Verify Count of Column between <Source> and <Target>")
    public void countOfColumnsVerify(String sourceQuery, String targetQuery) {
        ComparisonUtils.verifyTableCount(sourceQuery, targetQuery);

    }

    @Step("Verify if Column Size of <SourceTable> table in Source is matching with <TargetTable> table in Target")
    public void verifyColumnLength(String sourceTable, String targetTable){
        ComparisonUtils.verifyColumnSize(sourceTable,targetTable);

    }




}
