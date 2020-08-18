package utils;

import org.junit.Assert;

import java.util.HashMap;

public class AssertionUtils {

    public static void assertEquals(String oneValue, String secondValue) {
        Assert.assertEquals(oneValue, secondValue);
    }

    public static void assertTrue(HashMap<String,String> map1, HashMap<String,String> map2) {
       Assert.assertTrue(areEqual(map1,map2));
    }

    private static boolean areEqual(HashMap<String, String> first, HashMap<String, String> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream()
                .allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }


}
