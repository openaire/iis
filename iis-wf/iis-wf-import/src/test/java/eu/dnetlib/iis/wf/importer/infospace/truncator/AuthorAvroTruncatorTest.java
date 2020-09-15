package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AuthorAvroTruncatorTest {

    @Mock
    private Function<CharSequence, CharSequence> fullnameTruncator;

    @InjectMocks
    private AuthorAvroTruncator authorAvroTruncator = AuthorAvroTruncator.newBuilder().build();

    @Test
    public void shouldNotTruncateFullnameWhenItIsNull() {
        // given
        Author source = Author.newBuilder().build();

        // when
        Author result = authorAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(fullnameTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateFullnameWhenItIsNotNull() {
        // given
        Author source = Author.newBuilder()
                .setFullname("name1 name2")
                .build();
        when(fullnameTruncator.apply("name1 name2")).thenReturn("truncated");

        // when
        Author result = authorAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getFullname());
    }
}