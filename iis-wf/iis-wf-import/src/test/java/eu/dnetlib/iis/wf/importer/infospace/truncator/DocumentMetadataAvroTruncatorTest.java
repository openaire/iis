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
    private DocumentMetadataAvroTruncator.AbstractTruncator abstractTruncator;

    @Mock
    private DocumentMetadataAvroTruncator.TitleTruncator titleTruncator;

    @Mock
    private DocumentMetadataAvroTruncator.AuthorsTruncator authorsTruncator;

    @Mock
    private DocumentMetadataAvroTruncator.KeywordsTruncator keywordsTruncator;

    @InjectMocks
    private DocumentMetadataAvroTruncator documentMetadataAvroTruncator = new DocumentMetadataAvroTruncator();

    @Test
    public void shouldNotTruncateAbstractWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(abstractTruncator, never()).truncate(any());
    }

    @Test
    public void shouldTruncateAbstractWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        source.setAbstract$("abstract");
        when(abstractTruncator.truncate("abstract")).thenReturn("truncated");

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getAbstract$());
    }

    @Test
    public void abstractTruncatorShouldTruncateProperly() {
        // given
        Function<CharSequence, CharSequence> stringTruncator = mock(Function.class);

        DocumentMetadataAvroTruncator.AbstractTruncator abstractTruncator = new DocumentMetadataAvroTruncator.AbstractTruncator(-1);
        abstractTruncator.setStringTruncator(stringTruncator);

        // when
        abstractTruncator.truncate("abstract");

        // then
        verify(stringTruncator, times(1)).apply("abstract");
    }

    @Test
    public void shouldNotTruncateTitleWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(titleTruncator, never()).truncate(any());
    }

    @Test
    public void shouldTruncateTitleWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        source.setTitle("title");
        when(titleTruncator.truncate("title")).thenReturn("truncated");

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getTitle());
    }

    @Test
    public void titleTruncatorShouldTruncateProperly() {
        // given
        Function<CharSequence, CharSequence> stringTruncator = mock(Function.class);
        DocumentMetadataAvroTruncator.TitleTruncator titleTruncator = new DocumentMetadataAvroTruncator.TitleTruncator(-1);
        titleTruncator.setStringTruncator(stringTruncator);

        // when
        titleTruncator.truncate("title");

        // then
        verify(stringTruncator, times(1)).apply("title");
    }

    @Test
    public void shouldNotTruncateAuthorsWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(authorsTruncator, never()).truncate(any());
    }

    @Test
    public void shouldTruncateAuthorsWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        List<Author> authors = Collections.singletonList(mock(Author.class));
        source.setAuthors(authors);
        List<Author> truncated = Collections.singletonList(mock(Author.class));
        when(authorsTruncator.truncate(authors)).thenReturn(truncated);

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getAuthors());
    }

    @Test
    public void authorsTruncatorShouldTruncateProperly() {
        // given
        List<Author> authors = Collections.singletonList(mock(Author.class));
        Function<List<Author>, List<Author>> listTruncator = mock(Function.class);
        when(listTruncator.apply(authors)).thenReturn(Collections.singletonList(mock(Author.class)));
        AuthorAvroTruncator authorAvroTruncator = mock(AuthorAvroTruncator.class);
        DocumentMetadataAvroTruncator.AuthorsTruncator authorsTruncator = new DocumentMetadataAvroTruncator.AuthorsTruncator(-1, -1);
        authorsTruncator.setListTruncator(listTruncator);
        authorsTruncator.setAuthorAvroTruncator(authorAvroTruncator);

        // when
        authorsTruncator.truncate(authors);

        // then
        verify(authorAvroTruncator, times(1)).truncate(any(Author.class));
    }

    @Test
    public void shouldNotTruncateKeywordsWhenItIsNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(keywordsTruncator, never()).truncate(any());
    }

    @Test
    public void shouldTruncateKeywordsWhenItIsNotNull() {
        // given
        DocumentMetadata source = createDocumentMetadata();
        List<CharSequence> keywords = Collections.singletonList("keyword");
        source.setKeywords(keywords);
        List<CharSequence> truncated = Collections.singletonList("truncated");
        when(keywordsTruncator.truncate(keywords)).thenReturn(truncated);

        // when
        DocumentMetadata result = documentMetadataAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getKeywords());
    }

    @Test
    public void keywordsTruncatorShouldTruncateProperly() {
        // given
        List<CharSequence> keywords = Collections.singletonList("keyword");
        Function<List<CharSequence>, List<CharSequence>> listTruncator = mock(Function.class);
        when(listTruncator.apply(keywords)).thenReturn(Collections.singletonList("truncated"));
        Function<CharSequence, CharSequence> stringTruncator = mock(Function.class);
        DocumentMetadataAvroTruncator.KeywordsTruncator keywordsTruncator = new DocumentMetadataAvroTruncator.KeywordsTruncator(-1, -1);
        keywordsTruncator.setListTruncator(listTruncator);
        keywordsTruncator.setStringTruncator(stringTruncator);

        // when
        keywordsTruncator.truncate(keywords);

        // then
        verify(stringTruncator, times(1)).apply("truncated");
    }

    private static DocumentMetadata createDocumentMetadata() {
        return DocumentMetadata.newBuilder()
                .setId("id")
                .setPublicationType(PublicationType.newBuilder().build())
                .build();
    }
}