package eu.dnetlib.iis.common.utils;

import org.apache.spark.api.java.function.MapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Iterator conversion utils.
 */
public class IteratorUtils {

    private IteratorUtils() {
    }

    /**
     * Converts an iterator to list.
     *
     * @param iterator Iterator to be converted.
     * @param <E>      Type of elements supplied by iterator.
     * @return List with elements from iterator.
     */
    public static <E> List<E> toList(Iterator<E> iterator) {
        List<E> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        return list;
    }

    /**
     * Applies a given function to each elements of an iterator producing a list.
     *
     * @param iterator Iterator to be converted.
     * @param f        Function to apply to a each element from iterator.
     * @param <E>      Type of elements supplied by iterator.
     * @param <R>      Type of returned value.
     * @return List with elements from iterator after application of given functions.
     */
    public static <E, R> List<R> toList(Iterator<E> iterator, MapFunction<E, R> f) {
        List<R> list = new ArrayList<>();
        iterator.forEachRemaining(x -> {
            try {
                list.add(f.call(x));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return list;
    }

    /**
     * Converts an iterator to stream.
     *
     * @param iterator Iterator to be converted.
     * @param <E>      Type of elements supplied by iterator.
     * @return Stream  with elements from iterator.
     */
    public static <E> Stream<E> toStream(Iterator<E> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}