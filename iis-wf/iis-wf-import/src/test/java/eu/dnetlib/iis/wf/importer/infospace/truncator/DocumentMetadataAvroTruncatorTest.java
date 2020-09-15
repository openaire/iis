package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.PublicationType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DocumentMetadataAvroTruncatorTest {

    @Mock
    private Function<CharSequence, CharSequence> abstractTruncator;

    @Mock
    private Function<CharSequence, CharSequence> titleTruncator;

    @Mock
    private Function<List<Author>, List<Author>> authorsTruncator;

    @Mock
    private Function<List<CharSequence>, List<CharSequence>> keywordsTruncator;

    @InjectMocks
    private DocumentMetadataAvroTruncator documentMetadataAvroTruncator = DocumentMetadataAvroTruncator.newBuilder().build();

    @Test
    public void shouldNotTruncateAbstractWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(abstractTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateAbstractWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        source.setAbstract$("abstract");
        when(abstractTruncator.apply("abstract")).thenReturn("truncated");

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getAbstract$());
    }

    @Test
    public void shouldNotTruncateTitleWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(titleTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateTitleWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        source.setTitle("title");
        when(titleTruncator.apply("title")).thenReturn("truncated");

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getTitle());
    }

    @Test
    public void shouldNotTruncateAuthorsWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(authorsTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateAuthorsWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        List<Author> authors = Collections.singletonList(mock(Author.class));
        source.setAuthors(authors);
        List<Author> truncated = Collections.singletonList(mock(Author.class));
        when(authorsTruncator.apply(authors)).thenReturn(truncated);

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getAuthors());
    }

    @Test
    public void shouldNotTruncateKeywordsWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(keywordsTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateKeywordsWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        List<CharSequence> keywords = Collections.singletonList("keyword");
        source.setKeywords(keywords);
        List<CharSequence> truncated = Collections.singletonList("truncated");
        when(keywordsTruncator.apply(keywords)).thenReturn(truncated);

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getKeywords());
    }

    private static DocumentMetadata createDocumentMetadata() {
        return DocumentMetadata.newBuilder()
                .setId("id")
                .setPublicationType(PublicationType.newBuilder().build())
                .build();
    }
}