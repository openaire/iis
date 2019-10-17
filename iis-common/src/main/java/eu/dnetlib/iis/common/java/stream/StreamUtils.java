package eu.dnetlib.iis.common.java.stream;

import eu.dnetlib.iis.common.java.io.CloseableIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {

    public static <X> Stream<X> asStream(Iterator<X> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public static <X, R> R withCloseableIterator(CloseableIterator<X> iterator, Function<Stream<X>, R> f) throws IOException {
        try {
            return f.apply(asStream(iterator));
        } finally {
            iterator.close();
        }
    }
}
