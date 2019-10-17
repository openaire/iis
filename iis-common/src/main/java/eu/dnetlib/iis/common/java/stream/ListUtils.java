package eu.dnetlib.iis.common.java.stream;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ListUtils {

    private ListUtils() {
    }

    public static <X, Y> List<Pair<X, Y>> zip(List<X> left, List<Y> right) {
        if (left.size() != right.size()) {
            throw new IllegalArgumentException(String.format("List sizes do not match: left=%d, right=%d",
                    left.size(), right.size()));
        }
        return IntStream.range(0, left.size())
                .mapToObj(i -> Pair.of(left.get(i), right.get(i)))
                .collect(Collectors.toList());
    }

}
