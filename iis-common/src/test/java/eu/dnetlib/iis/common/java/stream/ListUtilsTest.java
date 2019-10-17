package eu.dnetlib.iis.common.java.stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ListUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void zipShouldThrowExceptionWhenListSizeNotMatch() {
        //given
        List<String> left = Arrays.asList("left 1", "left 2");
        List<String> right = Arrays.asList("right 1", "right 2", "right 3");

        //when
        ListUtils.zip(left, right);
    }

    @Test
    public void zipShouldZipLists() {
        //given
        List<String> left = Arrays.asList("left 1", "left 2", "left 3");
        List<String> right = Arrays.asList("right 1", "right 2", "right 3");

        //when
        List<Pair<String, String>> zipped = ListUtils.zip(left, right);

        //then
        assertEquals(left.size(), zipped.size());
        assertEquals(right.size(), zipped.size());
        for (int i = 0; i < zipped.size(); i++) {
            assertEquals(left.get(i), zipped.get(i).getLeft());
            assertEquals(right.get(i), zipped.get(i).getRight());
        }
    }
}
