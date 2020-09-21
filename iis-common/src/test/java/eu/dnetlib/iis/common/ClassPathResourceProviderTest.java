package eu.dnetlib.iis.common;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

import static eu.dnetlib.iis.common.ClassPathResourceProvider.*;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ClassPathResourceProviderTest {

    @Test(expected = RuntimeException.class)
    public void givenPathToResourceThatDoesNotExist_whenGetResourcePathIsCalled_thenExceptionIsThrown() {
        getResourcePath("path/to/resource");
    }

    @Test
    public void givenPathToResource_whenGetResourcePathIsCalled_thenProperPathIsReturned() {
        String location = "eu/dnetlib/iis/common/data/@a/static-resource-file.txt";

        assertThat(getResourcePath(location), endsWith(location));
    }

    @Test(expected = RuntimeException.class)
    public void givenPathToResourceThatDoesNotExist_whenGetResourceContentIsCalled_thenExceptionIsThrown() {
        getResourceContent("path/to/resource");
    }

    @Test
    public void givenPathToResource_whenGetResourceContentIsCalled_thenContentIsReturned() {
        assertEquals("This is a static resource file.",
                getResourceContent("eu/dnetlib/iis/common/data/@a/static-resource-file.txt"));
    }

    @Test
    public void givenPathsToResources_whenGetResourcesContentsIsCalled_thenContentsAreReturned() {
        assertEquals(Collections.singletonList("This is a static resource file."),
                getResourcesContents("eu/dnetlib/iis/common/data/@a/static-resource-file.txt"));
    }

    @Test(expected = RuntimeException.class)
    public void givenPathToResourceThatDoesNotExist_whenGetResourceInputStreamIsCalled_thenExceptionIsThrown() {
        getResourceInputStream("path/to/resource");
    }

    @Test
    public void givenPathToResource_whenGetResourceInputStreamIsCalled_thenInputStreamIsReturned() {
        String content = new BufferedReader(
                new InputStreamReader(getResourceInputStream("eu/dnetlib/iis/common/data/@a/static-resource-file.txt"),
                        StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));

        assertEquals("This is a static resource file.", content);
    }

    @Test
    public void givenPathToResource_whenGetResourceInputStreamReaderIsCalled_thenInputStreamReaderIsReturned() {
        String content = new BufferedReader(
                getResourceReader("eu/dnetlib/iis/common/data/@a/static-resource-file.txt"))
                .lines()
                .collect(Collectors.joining("\n"));

        assertEquals("This is a static resource file.", content);
    }
}