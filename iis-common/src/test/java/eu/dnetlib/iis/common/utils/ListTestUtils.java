package eu.dnetlib.iis.common.utils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Common list related test utility class.
 */
public class ListTestUtils {

    private ListTestUtils() {
    }

    /**
     * Zips two lists and compares elements.
     *
     * @param left  List of expected values.
     * @param right List of actual values.
     * @param <X>   Type of elements in the lists.
     */
    public static <X extends Comparable<X>> void compareLists(List<X> left, List<X> right) {
        ListUtils.zip(left, right).forEach(x -> assertEquals(x.getLeft(), x.getRight()));
    }
}
