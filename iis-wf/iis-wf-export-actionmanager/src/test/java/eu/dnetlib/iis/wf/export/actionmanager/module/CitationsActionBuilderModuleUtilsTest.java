package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CitationsActionBuilderModuleUtilsTest {

    @Test
    @DisplayName("CitationEntry is converted to BlobCitationEntry using value functions")
    public void givenCitationEntry_whenConvertedToBlobCitationEntry_thenBuildersAreUsed() {
        CitationEntry citationEntry = mock(CitationEntry.class);
        Function<CitationEntry, Integer> positionFn = mock(Function.class);
        when(positionFn.apply(citationEntry)).thenReturn(-1);
        Function<CitationEntry, String> rawTextFn = mock(Function.class);
        when(rawTextFn.apply(citationEntry)).thenReturn("raw text");
        Function<CitationEntry, List<TypedId>> identifiersFn = mock(Function.class);
        when(identifiersFn.apply(citationEntry)).thenReturn(Collections.singletonList(mock(TypedId.class)));

        CitationsActionBuilderModuleUtils.build(citationEntry, positionFn, rawTextFn, identifiersFn);

        verify(positionFn).apply(citationEntry);
        verify(rawTextFn).apply(citationEntry);
        verify(identifiersFn).apply(citationEntry);
    }

    @Nested
    public class PositionBuilderTest {

        @Test
        @DisplayName("Position is build from citation entry")
        public void givenCitationEntry_whenPositionValueIsBuild_thenProperValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, null, null, null, Collections.emptyMap());

            Integer result = CitationsActionBuilderModuleUtils.PositionBuilder.build(citationEntry);

            assertEquals(5, result);
        }
    }

    @Nested
    public class RawTextBuilderTest {

        @Test
        @DisplayName("Null raw text string is build from citation entry")
        public void givenCitationEntryWithNullRawText_whenRawTextValueIsBuild_thenNullValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, null, null, null, Collections.emptyMap());

            String result = CitationsActionBuilderModuleUtils.RawTextBuilder.build(citationEntry);

            assertNull(result);
        }

        @Test
        @DisplayName("Non-null raw text string is build from citation entry")
        public void givenCitationEntryWithNonNullRawText_whenRawTextValueIsBuild_thenNonNullValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, "raw text", null, null, Collections.emptyMap());

            String result = CitationsActionBuilderModuleUtils.RawTextBuilder.build(citationEntry);

            assertEquals("raw text", result);
        }
    }

    @Nested
    public class IdentifiersBuilderTest {

        @Test
        @DisplayName("Null identifiers are build from citation entry")
        public void givenCitationEntryWithInvalidDestinationDocumentIdAndInvalidExternalDestinationDocumentIds_whenIdentifiersValueIsBuild_thenNullValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, null, null, null, Collections.emptyMap());
            Function<CitationEntry, Boolean> destinationDocumentIdValidator = mock(Function.class);
            when(destinationDocumentIdValidator.apply(citationEntry)).thenReturn(false);
            Function<CitationEntry, TypedId> typedIdFromDestinationDocumentIdFn = mock(Function.class);
            Function<CitationEntry, Boolean> externalDestinationDocumentIdsValidator = mock(Function.class);
            when(externalDestinationDocumentIdsValidator.apply(citationEntry)).thenReturn(false);
            Function<CitationEntry, List<TypedId>> typedIdsFromExternalDestinationDocumentIdsFn = mock(Function.class);

            List<TypedId> result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.build(citationEntry,
                    destinationDocumentIdValidator,
                    typedIdFromDestinationDocumentIdFn,
                    externalDestinationDocumentIdsValidator,
                    typedIdsFromExternalDestinationDocumentIdsFn);

            assertNull(result);
            verify(typedIdFromDestinationDocumentIdFn, never()).apply(any());
            verify(typedIdsFromExternalDestinationDocumentIdsFn, never()).apply(any());
        }

        @Test
        @DisplayName("Non-null identifiers are build from citation entry with destination document id")
        public void givenCitationEntryWithValidDestinationDocumentIdAndInvalidExternalDestinationDocumentIds_whenIdentifiersValueIsBuild_thenProperValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, null, "destination document id", null, Collections.emptyMap());
            Function<CitationEntry, Boolean> destinationDocumentIdValidator = mock(Function.class);
            when(destinationDocumentIdValidator.apply(citationEntry)).thenReturn(true);
            TypedId typedId = mock(TypedId.class);
            Function<CitationEntry, TypedId> typedIdFromDestinationDocumentIdFn = mock(Function.class);
            when(typedIdFromDestinationDocumentIdFn.apply(citationEntry)).thenReturn(typedId);
            Function<CitationEntry, Boolean> externalDestinationDocumentIdsValidator = mock(Function.class);
            when(externalDestinationDocumentIdsValidator.apply(citationEntry)).thenReturn(false);
            Function<CitationEntry, List<TypedId>> typedIdsFromExternalDestinationDocumentIdsFn = mock(Function.class);

            List<TypedId> result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.build(citationEntry,
                    destinationDocumentIdValidator,
                    typedIdFromDestinationDocumentIdFn,
                    externalDestinationDocumentIdsValidator,
                    typedIdsFromExternalDestinationDocumentIdsFn);

            assertEquals(Collections.singletonList(typedId), result);
            verify(typedIdsFromExternalDestinationDocumentIdsFn, never()).apply(any());
        }

        @Test
        @DisplayName("Non-null identifiers are build from citation entry with destination document id and external destination document id")
        public void givenCitationEntryWithValidDestinationDocumentIdAndValidExternalDestinationDocumentIds_whenIdentifiersValueIsBuild_thenProperValueIsReturned() {
            CitationEntry citationEntry = buildCitationEntry(
                    5, null, "destination document id", null, Collections.emptyMap());
            Function<CitationEntry, Boolean> destinationDocumentIdValidator = mock(Function.class);
            when(destinationDocumentIdValidator.apply(citationEntry)).thenReturn(true);
            TypedId typedId1 = mock(TypedId.class);
            Function<CitationEntry, TypedId> typedIdFromDestinationDocumentIdFn = mock(Function.class);
            when(typedIdFromDestinationDocumentIdFn.apply(citationEntry)).thenReturn(typedId1);
            Function<CitationEntry, Boolean> externalDestinationDocumentIdsValidator = mock(Function.class);
            when(externalDestinationDocumentIdsValidator.apply(citationEntry)).thenReturn(true);
            TypedId typedId2 = mock(TypedId.class);
            Function<CitationEntry, List<TypedId>> typedIdsFromExternalDestinationDocumentIdsFn = mock(Function.class);
            when(typedIdsFromExternalDestinationDocumentIdsFn.apply(citationEntry)).thenReturn(Collections.singletonList(typedId2));

            List<TypedId> result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.build(citationEntry,
                    destinationDocumentIdValidator,
                    typedIdFromDestinationDocumentIdFn,
                    externalDestinationDocumentIdsValidator,
                    typedIdsFromExternalDestinationDocumentIdsFn);

            assertEquals(Arrays.asList(typedId1, typedId2), result);
        }

        @Nested
        public class TypedIdFromDestinationDocumentIdBuilderTest {

            @Test
            @DisplayName("Null destination document id is invalid")
            public void givenCitationEntryWithNullDestinationDocumentId_whenValidated_thenIsInvalid() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, null, null, Collections.emptyMap());

                Boolean result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdFromDestinationDocumentIdBuilder.isValid(
                        citationEntry);

                assertFalse(result);
            }

            @Test
            @DisplayName("Non-null destination document id is valid")
            public void givenCitationEntryWithNonNullDestinationDocumentId_whenValidated_thenIsValid() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, "destination document id", null, Collections.emptyMap());

                Boolean result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdFromDestinationDocumentIdBuilder.isValid(
                        citationEntry);

                assertTrue(result);
            }

            @Test
            @DisplayName("TypedId is build from citation entry using confidence level builder")
            public void givenCitationEntry_whenTypedIdValueIsBuild_thenConfidenceValueBuilderIsUsedAndProperValueIsReturned() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, "destination document id", null, Collections.emptyMap());
                Function<CitationEntry, Float> confidenceLevelFn = mock(Function.class);
                when(confidenceLevelFn.apply(citationEntry)).thenReturn(1f);

                TypedId result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdFromDestinationDocumentIdBuilder.build(
                        citationEntry, confidenceLevelFn);

                assertEquals(new TypedId("destination document id", ExtraInfoConstants.CITATION_TYPE_OPENAIRE, 1f), result);
            }

            @Nested
            public class ConfidenceLevelBuilderTest {

                @Test
                @DisplayName("Confidence level is build from citation entry with null confidence level")
                public void givenCitationEntryWithNullConfidenceLevel_whenConfidenceLevelValueIsBuild_thenProperValueIsReturned() {
                    CitationEntry citationEntry = buildCitationEntry(
                            5, null, "destination document id", null, Collections.emptyMap());

                    Float result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdFromDestinationDocumentIdBuilder.ConfidenceLevelBuilder.build(
                            citationEntry);

                    assertEquals(1f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
                }

                @Test
                @DisplayName("Confidence level is build from citation entry with non-null confidence level")
                public void givenCitationEntryWithNonNullConfidenceLevel_whenConfidenceLevelValueIsBuild_thenProperValueIsReturned() {
                    CitationEntry citationEntry = buildCitationEntry(
                            5, null, "destination document id", 0.5f, Collections.emptyMap());

                    Float result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdFromDestinationDocumentIdBuilder.ConfidenceLevelBuilder.build(
                            citationEntry);

                    assertEquals(0.5f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR, result);
                }
            }
        }

        @Nested
        public class TypedIdsFromExternalDestinationDocumentIdsBuilderTest {

            @Test
            @DisplayName("Empty external destination document ids is invalid")
            public void givenCitationEntryWithEmptyExternalDestinationDocumentIds_whenValidated_thenIsInvalid() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, null, null, Collections.emptyMap());

                Boolean result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdsFromExternalDestinationDocumentIdsBuilder.isValid(
                        citationEntry);

                assertFalse(result);
            }

            @Test
            @DisplayName("Non-empty external destination document ids is valid")
            public void givenCitationEntryWithNonEmptyExternalDestinationDocumentId_whenValidated_thenIsValid() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, null, null, Collections.singletonMap("key", "value"));

                Boolean result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdsFromExternalDestinationDocumentIdsBuilder.isValid(
                        citationEntry);

                assertTrue(result);
            }

            @Test
            @DisplayName("TypedIds are build from citation entry")
            public void givenCitationEntryWithNonEmptyExternalDestinationDocumentIds_whenTypedIdsValueIsBuild_thenProperValueIsReturned() {
                CitationEntry citationEntry = buildCitationEntry(
                        5, null, null, null, Collections.singletonMap("key", "value"));

                List<TypedId> result = CitationsActionBuilderModuleUtils.IdentifiersBuilder.TypedIdsFromExternalDestinationDocumentIdsBuilder.build(
                        citationEntry);

                assertEquals(
                        Collections.singletonList(new TypedId("value", "key", 1f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR)),
                        result);
            }
        }
    }

    private static CitationEntry buildCitationEntry(int position,
                                                    CharSequence rawText,
                                                    CharSequence destinationDocumentId,
                                                    Float confidenceLevel,
                                                    Map<CharSequence, CharSequence> externalDestinationDocumentIds) {
        return CitationEntry.newBuilder()
                .setPosition(position)
                .setRawText(rawText)
                .setDestinationDocumentId(destinationDocumentId)
                .setConfidenceLevel(confidenceLevel)
                .setExternalDestinationDocumentIds(externalDestinationDocumentIds)
                .build();
    }
}