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
        List<String> rs = Utils.getResultSetValues();
        if(rs.size() > 1) {
            for (int i = 0; i < rs.size(); i++) {

            }
        }
        else {
            AssertionUtils.assertCountEquals(String.valueOf(Utils.getCount()),String.valueOf(Integer.parseInt(rs.get(0))));
        }
    }


}
