package eu.dnetlib.iis.common.java.stream;

import eu.dnetlib.iis.common.java.io.CloseableIterator;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class StreamUtilsTest {

    @Test
    public void asStreamShouldConvertIteratorToStream() {
        //given
        List<String> elems = Arrays.asList("elem 1", "elem 2", "elem 3");

        //when
        Stream<String> elemStream = StreamUtils.asStream(elems.iterator());

        //then
        assertThat(elemStream.collect(Collectors.toList()), is(elems));
    }

    private static class CloseableIteratorString implements CloseableIterator<String> {
        private Iterator<String> iterator;
        private Closeable closeable;

        private CloseableIteratorString(Iterator<String> iterator, Closeable closeable) {
            this.iterator = iterator;
            this.closeable = closeable;
        }

        @Override
        public void close() throws IOException {
            closeable.close();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public String next() {
            return iterator.next();
        }
    }

    @Test
    public void withCloseableIteratorShouldApplyGivenFunction() throws IOException {
        //given
        List<String> elems = Arrays.asList("elem 1", "elem 2", "elem 3");
        Closeable closeable = mock(Closeable.class);
        CloseableIterator<String> iterator = new CloseableIteratorString(elems.iterator(), closeable);
        Function<Stream<String>, Stream<String>> f = Function.identity();

        //when
        Stream<String> stream = StreamUtils.withCloseableIterator(iterator, f);

        //then
        assertEquals(elems, stream.collect(Collectors.toList()));
        verify(closeable, times(1)).close();
    }
}