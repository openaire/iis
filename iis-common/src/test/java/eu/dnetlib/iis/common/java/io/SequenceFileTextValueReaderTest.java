package eu.dnetlib.iis.common.java.io;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * @author mhorst
 *
 */
public class SequenceFileTextValueReaderTest {

    private static final String SEQUENCE_FILES = "sequence_files";
    private static final String ONE_FILE = SEQUENCE_FILES+"/part-00000";

    //------------------------ TESTS --------------------------

    @Test
    public void readFile() throws Exception {
        // given
        SequenceFileTextValueReader reader = newReader(ONE_FILE);

        // execute
        ArrayList<String> items = readAll(reader);

        // assert
        assertThat(items, contains("a", "bb", "ccc"));
    }

    @Test
    public void readDirectory() throws Exception {
        // given
        SequenceFileTextValueReader reader = newReader(SEQUENCE_FILES);

        // execute
        ArrayList<String> items = readAll(reader);

        // assert
        assertThat(items,
            either(contains("a", "bb", "ccc", "d", "ee", "fff"))
            .or(contains("d", "ee", "fff", "a", "bb", "ccc")));
    }

    @Test
    public void closeReader() throws Exception {
        // given
        SequenceFileTextValueReader reader = newReader(ONE_FILE);

        // execute
        reader.close();

        // assert
        RuntimeException e = assertThrows(RuntimeException.class, reader::hasNext);
        assertEquals(IOException.class, e.getCause().getClass());
    }

    //------------------------ PRIVATE --------------------------

    private SequenceFileTextValueReader newReader(String resource) throws IOException, URISyntaxException {
        return new SequenceFileTextValueReader(new FileSystemPath(new Path(getClass().getResource(resource).toURI())));
    }

    private static ArrayList<String> readAll(SequenceFileTextValueReader reader) {
        ArrayList<String> items = new ArrayList<>();
        while (reader.hasNext()) {
            items.add(reader.next().toString());
        }
        return items;
    }
}
