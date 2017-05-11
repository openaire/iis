package eu.dnetlib.iis.common.java.io;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * @author mhorst
 *
 */
public class SequenceFileTextValueReaderTest {

    private static final String SEQUENCE_FILES = "sequence_files";
    private static final String ONE_FILE = SEQUENCE_FILES+"/part-00000";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        thrown.expectCause(isA(IOException.class));
        reader.hasNext();
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
