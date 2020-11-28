package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;

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
        CitationsExtraInfoConverter citationsExtraInfoConverter = mock(CitationsExtraInfoConverter.class);
        when(citationsExtraInfoConverter.serialize(blobCitationEntries)).thenReturn("value");
        module.setCitationsExtraInfoConverter(citationsExtraInfoConverter);

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
        private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchValidator citationEntryMatchValidator;

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
            CitationEntry citationEntry = mock(CitationEntry.class);
            CitationEntry normalizedCitationEntry = mock(CitationEntry.class);
            BlobCitationEntry blobCitationEntry = mock(BlobCitationEntry.class);
            when(trustLevelConverter.convert(trustLevelThreshold)).thenReturn(0.1f);
            when(citationEntryMatchValidator.validate(citationEntry, 0.1f)).thenReturn(true);
            when(citationEntryNormalizer.normalize(citationEntry, true)).thenReturn(normalizedCitationEntry);
            when(blobCitationEntryBuilder.build(normalizedCitationEntry)).thenReturn(blobCitationEntry);

            SortedSet<BlobCitationEntry> result = citationEntriesConverter.convert(
                    Collections.singletonList(citationEntry), trustLevelThreshold);

            assertEquals(1, result.size());
            assertSame(blobCitationEntry, result.first());
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
        public class CitationEntryMatchValidatorTest {

            @Mock
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchValidator.ConfidenceLevelValidator confidenceLevelValidator;

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchValidator citationEntryMatchValidator;

            @Test
            @DisplayName("Citation entry with null confidence level is not valid")
            public void givenValidator_whenCitationEntryWithNullConfidenceLevelIsConverted_thenFalseIsReturned() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(null);

                Boolean result = citationEntryMatchValidator.validate(citationEntry, 0.5f);

                assertFalse(result);
            }

            @Test
            @DisplayName("Citation entry with non-null confidence level is validated against threshold")
            public void givenValidator_whenCitationEntryWithNonNullConfidenceLevelIsConverted_thenThresholdValidatorIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);
                when(citationEntry.getConfidenceLevel()).thenReturn(0.1f);

                citationEntryMatchValidator.validate(citationEntry, 0.5f);

                verify(confidenceLevelValidator, atLeastOnce()).validate(0.1f, 0.5f);
            }

            @Nested
            public class ConfidenceLevelValidatorTest {

                @Mock
                private BiFunction<Float, Float, Boolean> thresholdValidatorFn;

                @InjectMocks
                private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryMatchValidator.ConfidenceLevelValidator confidenceLevelValidator;

                @Test
                @DisplayName("Non-null confidence level is validated using validator")
                public void givenValidator_whenNonNullConfidenceLevelIsValidated_thenValidatorIsUsed() {
                    confidenceLevelValidator.validate(0.1f, trustLevelThreshold);

                    verify(thresholdValidatorFn, atLeastOnce()).apply(0.1f, trustLevelThreshold);
                }
            }
        }

        @Nested
        public class CitationEntryNormalizerTest {

            @Mock
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.ValidMatchCitationEntryNormalizer validMatchCitationEntryNormalizer;

            @Mock
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.InvalidMatchCitationEntryNormalizer invalidMatchCitationEntryNormalizer;

            @InjectMocks
            private CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer citationEntryNormalizer;

            @Test
            @DisplayName("Valid match citation entry is normalized using normalizer")
            public void givenNormalizer_whenValidMatchCitationEntryIsNormalized_thenProperNormalizerIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);

                citationEntryNormalizer.normalize(citationEntry, true);

                verify(validMatchCitationEntryNormalizer, atLeastOnce()).normalize(citationEntry);
                verify(invalidMatchCitationEntryNormalizer, never()).normalize(citationEntry);
            }

            @Test
            @DisplayName("Invalid match citation entry is normalized using normalizer")
            public void givenNormalizer_whenInvalidMatchCitationEntryIsNormalized_thenProperNormalizerIsUsed() {
                CitationEntry citationEntry = mock(CitationEntry.class);

                citationEntryNormalizer.normalize(citationEntry, false);

                verify(validMatchCitationEntryNormalizer, never()).normalize(citationEntry);
                verify(invalidMatchCitationEntryNormalizer, atLeastOnce()).normalize(citationEntry);
            }

            @Nested
            public class ValidMatchCitationEntryNormalizerTest {

                @Test
                @DisplayName("Valid match citation entry is properly normalized")
                public void givenNormalizer_whenCitationEntryIsNormalized_thenProperResultIsReturned() {
                    CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.ValidMatchCitationEntryNormalizer validMatchCitationEntryNormalizer =
                            new CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.ValidMatchCitationEntryNormalizer();
                    CitationEntry citationEntry = mock(CitationEntry.class);
                    when(citationEntry.getDestinationDocumentId()).thenReturn("prefix|destination document id");

                    CitationEntry result = validMatchCitationEntryNormalizer.normalize(citationEntry);

                    assertSame(citationEntry, result);
                    verify(citationEntry, atLeastOnce()).setDestinationDocumentId("destination document id");
                }
            }

            @Nested
            public class InvalidMatchCitationEntryNormalizerTest {

                @Test
                @DisplayName("Invalid match citation entry is properly normalized")
                public void givenNormalizer_whenCitationEntryIsNormalized_thenProperResultIsReturned() {
                    CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.InvalidMatchCitationEntryNormalizer invalidMatchCitationEntryNormalizer =
                            new CitationsActionBuilderModuleFactory.CitationEntriesConverter.CitationEntryNormalizer.InvalidMatchCitationEntryNormalizer();
                    CitationEntry citationEntry = mock(CitationEntry.class);

                    CitationEntry result = invalidMatchCitationEntryNormalizer.normalize(citationEntry);

                    assertSame(citationEntry, result);
                    verify(citationEntry, atLeastOnce()).setDestinationDocumentId(null);
                    verify(citationEntry, atLeastOnce()).setConfidenceLevel(null);
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
