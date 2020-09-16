package eu.dnetlib.iis.wf.importer.infospace.truncator;

import eu.dnetlib.iis.importer.schemas.DataSetReference;
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
public class DataSetReferenceAvroTruncatorTest {
    @Mock
    private Function<List<CharSequence>, List<CharSequence>> creatorNamesTruncator;

    @Mock
    private Function<List<CharSequence>, List<CharSequence>> titlesTruncator;

    @Mock
    private Function<CharSequence, CharSequence> descriptionTruncator;

    @InjectMocks
    private DataSetReferenceAvroTruncator dataSetReferenceAvroTruncator = DataSetReferenceAvroTruncator.newBuilder().build();

    @Test
    public void shouldNotTruncateCreatorNamesWhenItIsNull() {
        // given
        DataSetReference source = createDataSetReference();

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(creatorNamesTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateCreatorNamesWhenItIsNotNull() {
        // given
        DataSetReference source = createDataSetReference();
        List<CharSequence> creatorNames = Collections.singletonList("creator name");
        source.setCreatorNames(creatorNames);
        List<CharSequence> truncated = Collections.singletonList("truncated");
        when(creatorNamesTruncator.apply(creatorNames)).thenReturn(truncated);

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getCreatorNames());
    }

    @Test
    public void shouldNotTruncateTitlesWhenItIsNull() {
        // given
        DataSetReference source = createDataSetReference();

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(titlesTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateTitlesWhenItIsNotNull() {
        // given
        DataSetReference source = createDataSetReference();
        List<CharSequence> titles = Collections.singletonList("title");
        source.setTitles(titles);
        List<CharSequence> truncated = Collections.singletonList("truncated");
        when(titlesTruncator.apply(titles)).thenReturn(truncated);

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals(truncated, result.getTitles());
    }

    @Test
    public void shouldNotTruncateDescriptionWhenItIsNull() {
        // given
        DataSetReference source = createDataSetReference();

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals(source, result);
        verify(descriptionTruncator, never()).apply(any());
    }

    @Test
    public void shouldTruncateAbstractWhenItIsNotNull() {
        // given
        DataSetReference source = createDataSetReference();
        source.setDescription("description");
        when(descriptionTruncator.apply("description")).thenReturn("truncated");

        // when
        DataSetReference result = dataSetReferenceAvroTruncator.truncate(source);

        // then
        assertEquals("truncated", result.getDescription());
    }


    private static DataSetReference createDataSetReference() {
        return DataSetReference.newBuilder()
                .setId("id")
                .setReferenceType("reference type")
                .setIdForGivenType("id for given type")
                .build();
    }
}