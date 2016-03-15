package eu.dnetlib.iis.wf.citationmatching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.ReferenceMetadata;
import eu.dnetlib.iis.wf.citationmatching.converter.ReferenceMetadataToMatchableConverter;
import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Option;
import scala.Tuple2;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class ReferenceMetadataInputConverterTest {

    private ReferenceMetadataInputConverter referenceMetadataInputConverter = new ReferenceMetadataInputConverter();

    @Mock
    private ReferenceMetadataToMatchableConverter converter;


    @Mock
    private JavaPairRDD<String, ReferenceMetadata> inputCitations;

    @Mock
    private JavaPairRDD<String, MatchableEntity> convertedCitations;

    @Captor
    private ArgumentCaptor<PairFlatMapFunction<Tuple2<String, ReferenceMetadata>, String, MatchableEntity>> convertCitationFunction;


    @Before
    public void setup() {
        referenceMetadataInputConverter.setConverter(converter);
    }


    //------------------------ TESTS --------------------------

    @Test
    public void convertCitations() throws Exception {

        // given
        doReturn(convertedCitations).when(inputCitations).flatMapToPair(any());


        // execute

        JavaPairRDD<String, MatchableEntity> retConvertedCitations = referenceMetadataInputConverter.convertCitations(inputCitations);


        // assert

        assertTrue(retConvertedCitations == convertedCitations);

        verify(inputCitations).flatMapToPair(convertCitationFunction.capture());
        assertConvertCitationFunction(convertCitationFunction.getValue());
        assertConvertCitationFunction_TOO_LONG_RAW_TEXT(convertCitationFunction.getValue());

    }


    //------------------------ PRIVATE --------------------------

    private void assertConvertCitationFunction(PairFlatMapFunction<Tuple2<String, ReferenceMetadata>, String, MatchableEntity> function) throws Exception {
        ReferenceMetadata referenceMetadata = mock(ReferenceMetadata.class);
        MatchableEntity matchableEntity = mock(MatchableEntity.class);

        when(converter.convertToMatchableEntity("cit_id_3", referenceMetadata)).thenReturn(matchableEntity);
        doReturn(Option.apply("some raw text")).when(matchableEntity).rawText();


        Iterable<Tuple2<String, MatchableEntity>> retConverted = function.call(new Tuple2<>("cit_id_3", referenceMetadata));


        List<Tuple2<String, MatchableEntity>> retConvertedList = Lists.newArrayList(retConverted);
        assertEquals(1, retConvertedList.size());
        assertTrue(retConvertedList.get(0)._2 == matchableEntity);
        assertEquals("cit_id_3", retConvertedList.get(0)._1);

    }
    
    private void assertConvertCitationFunction_TOO_LONG_RAW_TEXT(PairFlatMapFunction<Tuple2<String, ReferenceMetadata>, String, MatchableEntity> function) throws Exception {
        ReferenceMetadata referenceMetadata = mock(ReferenceMetadata.class);
        MatchableEntity matchableEntity = mock(MatchableEntity.class);

        when(converter.convertToMatchableEntity("cit_id_3", referenceMetadata)).thenReturn(matchableEntity);
        doReturn(Option.apply(StringUtils.repeat('a', 10001))).when(matchableEntity).rawText();


        Iterable<Tuple2<String, MatchableEntity>> retConverted = function.call(new Tuple2<>("cit_id_3", referenceMetadata));


        List<Tuple2<String, MatchableEntity>> retConvertedList = Lists.newArrayList(retConverted);
        assertEquals(0, retConvertedList.size());

    }
}
