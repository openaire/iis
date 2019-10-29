package eu.dnetlib.iis.common.utils;

import org.junit.ComparisonFailure;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ListTestUtilsTest {

    @Test(expected = ComparisonFailure.class)
    public void compareShouldThrowExceptionWhenListsNotMatch() {
        //given
        List<String> left = Arrays.asList("a", "b");
        List<String> right = Arrays.asList("a", "x");

        //when
        ListTestUtils.compareLists(left, right);
    }

    @Test
    public void compareShouldNotThrowExceptionWhenListsMatch() {
        //given
        List<String> left = Arrays.asList("a", "b");
        List<String> right = Arrays.asList("a", "b");

        //when
        ListTestUtils.compareLists(left, right);
    }
}
