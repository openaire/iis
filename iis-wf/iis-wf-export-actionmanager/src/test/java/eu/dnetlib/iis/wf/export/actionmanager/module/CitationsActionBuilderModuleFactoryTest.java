package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoSerDe;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.module.CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchChecker.CheckResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 */
@ExtendWith(MockitoExtension.class)
public class CitationsActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<Citations, Result> {


    private String docId = "documentId";


    // ----------------------- CONSTRUCTORS -------------------


    public CitationsActionBuilderModuleFactoryTest() throws Exception {
        super(CitationsActionBuilderModuleFactory.class, AlgorithmName.document_referencedDocuments);
    }

    // ----------------------- TESTS --------------------------

    @Test
    @DisplayName("Empty atomic action list is build from citations with empty citation entry list")
    public void testBuildEmptyCitations() throws Exception {
        // given
        ActionBuilderModule<Citations, Result> module = factory.instantiate(config);

        // execute
        List<AtomicAction<Result>> actions = module.build(
                Citations.newBuilder().setCitations(Collections.emptyList()).setDocumentId(docId).build());

        // assert
        assertNotNull(actions);
        assertEquals(0, actions.size());
    }

    @Test
    @DisplayName("Non-empty atomic action list is build from citations with non-empty citation entry list")
    public void testBuild() throws Exception {
        // given
        CitationsActionBuilderModuleFactory.CitationActionBuilderModule module = (CitationsActionBuilderModuleFactory.CitationActionBuilderModule)
                factory.instantiate(config);
        CitationEntry citationEntry = buildCitationEntry(0.9f);
        List<CitationEntry> citationEntries = Collections.singletonList(citationEntry);
        Citations citations = Citations.newBuilder()
                .setDocumentId(docId)
                .setCitations(citationEntries)
                .build();
        TreeSet<BlobCitationEntry> blobCitationEntries = new TreeSet<>(Collections.singletonList(mock(BlobCitationEntry.class)));
        CitationsActionBuilderModuleFactory.CitationEntriesConverter citationEntriesConverter =
                mock(CitationsActionBuilderModuleFactory.CitationEntriesConverter.class);
        when(citationEntriesConverter.convert(citationEntries, trustLevelThreshold)).thenReturn(blobCitationEntries);
        module.setCitationEntriesConverter(citationEntriesConverter);
        CitationsExtraInfoSerDe citationsExtraInfoConverter = mock(CitationsExtraInfoSerDe.class);
        when(citationsExtraInfoConverter.serialize(blobCitationEntries)).thenReturn("value");
        module.setCitationsExtraInfoSerDe(citationsExtraInfoConverter);

        // execute
        List<AtomicAction<Result>> actions = module.build(citations);

        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction<Result> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Result.class, action.getClazz());
        Result result = action.getPayload();
        assertNotNull(result);
        assertEquals(docId, result.getId());
        assertNotNull(result.getExtraInfo());
        assertEquals(1, result.getExtraInfo().size());
        ExtraInfo extraInfo = result.getExtraInfo().get(0);
        assertNotNull(extraInfo);
        assertEquals(ExtraInfoConstants.NAME_CITATIONS, extraInfo.getName());
        assertEquals(ExtraInfoConstants.TYPOLOGY_CITATIONS, extraInfo.getTypology());
        assertEquals(StaticConfigurationProvider.ACTION_TRUST_0_9, extraInfo.getTrust());
        assertEquals(((AbstractActionBuilderFactory<Citations, Result>) factory).buildInferenceProvenance(), extraInfo.getProvenance());
        assertEquals("value", extraInfo.getValue());
    }

    @Nested
    public class CitationEntriesConverterTest {

        @Mock
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.TrustLevelConverter trustLevelConverter;

        @Mock
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchChecker citationEntryMatchChecker;

        @Mock
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.ConfidenceLevelValidator confidenceLevelValidator;

        @Mock
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer citationEntryNormalizer;

        @Mock
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.BlobCitationEntryBuilder blobCitationEntryBuilder;

        @InjectMocks
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter citationEntriesConverter;

        @Test
        @DisplayName("Null citation entries are converted to null")
        public void givenConverter_whenNullCitationEntriesAreConverted_thenNullIsReturned() {
            assertNull(citationEntriesConverter.convert(null, trustLevelThreshold));
        }

        @Test
        @DisplayName("Empty citation entries are converted to empty blob citation entries")
        public void givenConverter_whenEmptyCitationEntriesAreConverted_thenEmptyCollectionIsReturned() {
            assertTrue(citationEntriesConverter.convert(Collections.emptyList(), trustLevelThreshold).isEmpty());

            verify(trustLevelConverter, atLeastOnce()).convert(trustLevelThreshold);
        }

        @Test
        @DisplayName("Non-empty citation entries are converted to non-empty blob citation entries")
        public void givenConverter_whenNonEmptyCitationEntriesAreConverted_thenNonEmptyCollectionIsReturned() {
            CitationEntry notMatchingResultCitationEntry = mock(CitationEntry.class);
            CitationEntry matchingResultCitationEntry = mock(CitationEntry.class);
            CitationEntry normalizedMatchingResultCitationEntry = mock(CitationEntry.class);
            BlobCitationEntry blobCitationEntryForNotMatchingResultCitationEntry = mock(BlobCitationEntry.class);
            BlobCitationEntry blobCitationEntryForMatchingResultCitationEntry = mock(BlobCitationEntry.class);
            when(trustLevelConverter.convert(trustLevelThreshold)).thenReturn(0.1f);
            when(citationEntryMatchChecker.check(notMatchingResultCitationEntry)).thenReturn(CheckResult.NO_MATCHING_RESULT);
            when(citationEntryMatchChecker.check(matchingResultCitationEntry)).thenReturn(CheckResult.MATCHING_RESULT);
            when(confidenceLevelValidator.validate(CheckResult.NO_MATCHING_RESULT, notMatchingResultCitationEntry, 0.1f))
                    .thenReturn(true);
            when(confidenceLevelValidator.validate(CheckResult.MATCHING_RESULT, matchingResultCitationEntry, 0.1f))
                    .thenReturn(true);
            when(citationEntryNormalizer.normalize(CheckResult.NO_MATCHING_RESULT, notMatchingResultCitationEntry))
                    .thenReturn(notMatchingResultCitationEntry);
            when(citationEntryNormalizer.normalize(CheckResult.MATCHING_RESULT, matchingResultCitationEntry))
                    .thenReturn(normalizedMatchingResultCitationEntry);
            when(blobCitationEntryBuilder.build(notMatchingResultCitationEntry)).thenReturn(
                    blobCitationEntryForNotMatchingResultCitationEntry);
            when(blobCitationEntryBuilder.build(normalizedMatchingResultCitationEntry)).thenReturn(
                    blobCitationEntryForMatchingResultCitationEntry);

            SortedSet<BlobCitationEntry> result = citationEntriesConverter.convert(
                    Arrays.asList(notMatchingResultCitationEntry, matchingResultCitationEntry), trustLevelThreshold);

            assertEquals(2, result.size());
            assertThat(result, hasItems(blobCitationEntryForNotMatchingResultCitationEntry,
                    blobCitationEntryForMatchingResultCitationEntry));
        }

        @Nested
        public class TrustLevelConverterTest {

            @Test
            @DisplayName("Null trust level threshold is converted to null")
            public void givenConverter_whenNullValueIsConverted_thenNullIsReturned() {
                CitationsActionBuilderModuleFactory.CitationEntriesConverter.TrustLevelConverter trustLevelConverter =
                        new CitationsActionBuilderModuleFactory.CitationEntriesConverter.TrustLevelConverter(0.9f);

                Float result = trustLevelConverter.convert(null);

                assertNull(result);
            }

            @Test
            @DisplayName("Trust level threshold is converted to confidence level threshold using scaling factor")
            public void givenConverter_whenAFloatValueIsConverted_thenProperValueIsReturned() {
                CitationsActionBuilderModuleFactory.CitationEntriesConverter.TrustLevelConverter trustLevelConverter =
                        new CitationsActionBuilderModuleFactory.CitationEntriesConverter.TrustLevelConverter(0.9f);

                Float result = trustLevelConverter.convert(trustLevelThreshold);

                assertEquals(trustLevelThreshold / 0.9f, result);
            }
        }

        @Nested
        public class CitationEntryMatchCheckerTest {

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchChecker citationEntryMatchChecker;

            @Test
            @DisplayName("Citation entry with null confidence level does not contain a result of citation matching")
            public void givenChecker_whenCitationEntryWithNullConfidenceLevelIsChecked_thenCitationEntryDoesNotContainAMatchingResult() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(null);

                CheckResult result = citationEntryMatchChecker.check(citationEntry);

                assertEquals(CheckResult.NO_MATCHING_RESULT, result);
            }

            @Test
            @DisplayName("Citation entry with null destination document id does not contain a result of citation matching")
            public void givenChecker_whenCitationEntryWithNullDestinationDocumentIdIsChecked_thenCitationEntryDoesNotContainAMatchingResult() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(0.1f);
                when(citationEntry.getDestinationDocumentId()).thenReturn(null);

                CheckResult result = citationEntryMatchChecker.check(citationEntry);

                assertEquals(CheckResult.NO_MATCHING_RESULT, result);
            }

            @Test
            @DisplayName("Citation entry with non null confidence level and non null destination document id contains a result of citation matching")
            public void givenChecker_whenCitationEntryWithNonNullConfidenceLevelAndDestinationDocumentIdIsChecked_thenCitationEntryContainsAMatchingResult() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(0.1f);
                when(citationEntry.getDestinationDocumentId()).thenReturn("destination document id");

                CheckResult result = citationEntryMatchChecker.check(citationEntry);

                assertEquals(CheckResult.MATCHING_RESULT, result);
            }
        }

        @Nested
        public class ConfidenceLevelValidatorTest {

            @Mock
            private BiFunction<Float, Float, Boolean> thresholdValidatorFn;

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.ConfidenceLevelValidator confidenceLevelValidator;

            @Test
            @DisplayName("Citation entry without a result of citation matching is valid against any threshold")
            public void givenValidator_whenCitationEntryThatWithoutCitationMatchingResultIsValidated_thenTrueIsReturned() {
                Boolean result = confidenceLevelValidator.validate(CheckResult.NO_MATCHING_RESULT, mock(CitationEntry.class), trustLevelThreshold);

                assertTrue(result);
                verify(thresholdValidatorFn, never()).apply(anyFloat(), eq(trustLevelThreshold));
            }

            @Test
            @DisplayName("Citation entry with a result of citation matching is validated using utils")
            public void givenValidator_whenCitationEntryWithACitationMatchingResultIsValidated_thenUtilsIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(0.1f);

                confidenceLevelValidator.validate(CheckResult.MATCHING_RESULT, citationEntry, trustLevelThreshold);

                verify(thresholdValidatorFn, atLeastOnce()).apply(0.1f, trustLevelThreshold);
            }
        }

        @Nested
        public class CitationEntryNormalizerTest {

            @Mock
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.MatchingResultCitationEntryNormalizer matchingResultCitationEntryNormalizer;

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer citationEntryNormalizer;

            @Test
            @DisplayName("Citation entry without a result of citation matching is not normalized")
            public void givenNormalizer_whenCitationEntryWithoutACitationMatchingResultIsNormalized_thenTheSameInstanceIsReturned() {
                CitationEntry citationEntry = mock(CitationEntry.class);

                CitationEntry result = citationEntryNormalizer.normalize(CheckResult.NO_MATCHING_RESULT, citationEntry);

                assertSame(result, citationEntry);
                verify(matchingResultCitationEntryNormalizer, never()).normalize(citationEntry);
            }

            @Test
            @DisplayName("Citation entry with a result of citation matching is normalized using normalizer")
            public void givenNormalizer_whenCitationEntryWithACitationMatchingResultIsNormalized_thenProperNormalizerIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);

                citationEntryNormalizer.normalize(CheckResult.MATCHING_RESULT, citationEntry);

                verify(matchingResultCitationEntryNormalizer, atLeastOnce()).normalize(citationEntry);
            }

            @Nested
            public class MatchingResultCitationEntryNormalizerTest {

                @Test
                @DisplayName("Citation entry is properly normalized")
                public void givenNormalizer_whenCitationEntryIsNormalized_thenProperResultIsReturned() {
                    CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.MatchingResultCitationEntryNormalizer matchingResultCitationEntryNormalizer =
                            new CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.MatchingResultCitationEntryNormalizer();
                    CitationEntry citationEntry = mock(CitationEntry.class);
                    when(citationEntry.getDestinationDocumentId()).thenReturn("prefix|destination document id");

                    CitationEntry result = matchingResultCitationEntryNormalizer.normalize(citationEntry);

                    assertSame(citationEntry, result);
                    verify(citationEntry, atLeastOnce()).setDestinationDocumentId("destination document id");
                }
            }
        }

        @Nested
        public class BlobCitationEntryBuilderTest {

            @Mock
            private Function<CitationEntry, BlobCitationEntry> builderFn;

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.BlobCitationEntryBuilder blobCitationEntryBuilder;

            @Test
            @DisplayName("Blob citation entry is build from citation entry")
            public void givenBuilder_whenBlobCitationEntryIsBuild_thenBuilderIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);

                blobCitationEntryBuilder.build(citationEntry);

                verify(builderFn, atLeastOnce()).apply(citationEntry);
            }
        }
    }

    // ----------------------- PRIVATE --------------------------

    private CitationEntry buildCitationEntry(Float confidenceLevel) {
        CitationEntry.Builder citationEntryBuilder = CitationEntry.newBuilder();
        citationEntryBuilder.setPosition(1);
        citationEntryBuilder.setRawText("citation raw text");
        citationEntryBuilder.setDestinationDocumentId("50|dest-id");
        citationEntryBuilder.setExternalDestinationDocumentIds(Collections.singletonMap("extIdType", "extIdValue"));
        citationEntryBuilder.setConfidenceLevel(confidenceLevel);
        return citationEntryBuilder.build();
    }
}
