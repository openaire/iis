package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.java.stream.ListUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ListTestUtils {

    private ListTestUtils() {
    }

    public static <X extends Comparable<X>> void compareLists(List<X> left, List<X> right) {
        ListUtils.zip(left, right).forEach(x -> assertEquals(x.getLeft(), x.getRight()));
    }
}
