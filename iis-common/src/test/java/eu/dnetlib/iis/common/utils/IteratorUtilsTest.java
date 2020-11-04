package eu.dnetlib.iis.common.utils;

import org.apache.spark.api.java.function.MapFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IteratorUtilsTest {

    @Nested
    public class ToListTest {

        @Test
        @DisplayName("Iterator is converted to list")
        public void givenIterator_whenConvertedToList_thenListIsReturned() {
            List<String> list = Arrays.asList("elem 1", "elem 2", "elem 3");

            List<String> result = IteratorUtils.toList(list.iterator());

            assertEquals(list, result);
        }

        @Test
        @DisplayName("Iterator is converted to list and a mapping is applied to each element")
        public void givenIteratorAndMapFunction_whenConvertedToList_thenListOfMappedElementsIsReturned() {
            List<String> list = Arrays.asList("1", "2", "3");

            List<Integer> result = IteratorUtils.toList(list.iterator(), (MapFunction<String, Integer>) Integer::valueOf);

            assertEquals(Arrays.asList(1, 2, 3), result);
        }

        @Test
        @DisplayName("Exception is thrown")
        public void givenIteratorAndThrowingMapFunction_whenConvertedToList_thenExceptionIsThrown() {
            assertThrows(RuntimeException.class, () ->
                    IteratorUtils.toList(Arrays.asList("1", "2", "3").iterator(), (MapFunction<String, Object>) value -> {
                        throw new Exception();
                    })
            );
        }
    }

    @Nested
    public class ToStreamTest {

        @Test
        @DisplayName("Iterator is converted to stream")
        public void givenIterator_whenConvertedToStream_thenStreamIsReturned() {
            List<String> list = Arrays.asList("elem 1", "elem 2", "elem 3");

            List<String> result = IteratorUtils.toStream(list.iterator()).collect(Collectors.toList());

            assertEquals(result, list);
        }
    }
}