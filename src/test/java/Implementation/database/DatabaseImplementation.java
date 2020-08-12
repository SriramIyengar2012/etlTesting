package Implementation.database;

import com.thoughtworks.gauge.*;
import org.junit.Assert;
import utils.ComparisonUtils;
import utils.Utils;
import utils.Utils.*;

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
            Assert.assertEquals(Utils.getCount(),Integer.parseInt(rs.get(0)));
        }
    }


}
