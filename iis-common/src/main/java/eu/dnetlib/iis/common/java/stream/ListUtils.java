package eu.dnetlib.iis.common.java.stream;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Common list related utility class.
 */
public class ListUtils {

    private ListUtils() {
    }

    /**
     * Joins two lists of equal length into a single list of paired elements. Throws an exception when lists do not have equal size.
     *
     * @param left  List of elements to zip as left values.
     * @param right List of elements to zip as right values.
     * @param <X>   Type of elements in left list.
     * @param <Y>   Type of elements in right list.
     * @return List of pairs of elements.
     */
    public static <X, Y> List<Pair<X, Y>> zip(List<X> left, List<Y> right) {
        if (left.size() != right.size()) {
            throw new IllegalArgumentException(String.format("List sizes do not match: left=%d, right=%d",
                    left.size(), right.size()));
        }
        return IntStream.range(0, left.size())
                .mapToObj(i -> Pair.of(left.get(i), right.get(i)))
                .collect(Collectors.toList());
    }

    /**
     * Joins each element in given list with its index.
     *
     * @param list List of elements to zip with index.
     * @param <X>  Type of elements in list.
     * @return List of pairs of indices and matching elements.
     */
    public static <X> List<Pair<Integer, X>> zipWithIndex(List<X> list) {
        return IntStream.range(0, list.size())
                .mapToObj(i -> Pair.of(i, list.get(i)))
                .collect(Collectors.toList());
    }
}
