package eu.dnetlib.iis.common.java.stream;

import eu.dnetlib.iis.common.java.io.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Common stream related utility class.
 */
public class StreamUtils {

    private StreamUtils() {
    }

    /**
     * Converts an iterator to stream.
     *
     * @param iterator Iterator to convert.
     * @param <X>      Type of elements in iterator.
     * @return Stream of elements corresponding to given iterator.
     */
    public static <X> Stream<X> asStream(Iterator<X> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    /**
     * Applies a given function to stream of elements of a closable iterator, closing the iterator afterwards.
     *
     * @param iterator Iterator to take elements from.
     * @param f        Function to apply to a stream of elements.
     * @param <X>      Type of elements in iterator.
     * @param <R>      Type of returned value.
     * @return Returns the result of application of function to stream of elements.
     * @throws IOException
     */
    public static <X, R> R withCloseableIterator(CloseableIterator<X> iterator, Function<Stream<X>, R> f) throws IOException {
        try {
            return f.apply(asStream(iterator));
        } finally {
            iterator.close();
        }
    }
}
