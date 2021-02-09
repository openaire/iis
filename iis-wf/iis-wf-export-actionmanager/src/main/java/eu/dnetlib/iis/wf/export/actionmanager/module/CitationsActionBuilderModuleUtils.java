package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.conversion.ConfidenceAndTrustLevelConversionUtils;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import eu.dnetlib.iis.export.schemas.Citations;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link Citations} action builder module utilities.
 *
 * @author mhorst
 */
public final class CitationsActionBuilderModuleUtils {

    // ------------------------- CONSTRUCTORS ----------------------------

    private CitationsActionBuilderModuleUtils() {
    }

    // ------------------------- LOGIC -----------------------------------

    /**
     * Creates {@link BlobCitationEntry} from {@link CitationEntry}.
     * Translates confidence level into trust level applying confidenceToTrustLevelFactor.
     */
    public static BlobCitationEntry build(CitationEntry citationEntry) {
        return build(citationEntry, PositionBuilder::build, RawTextBuilder::build, IdentifiersBuilder::build);
    }

    public static BlobCitationEntry build(CitationEntry citationEntry,
                                          Function<CitationEntry, Integer> positionFn,
                                          Function<CitationEntry, String> rawTextFn,
                                          Function<CitationEntry, List<TypedId>> identifiersFn) {
        BlobCitationEntry blobCitationEntry = new BlobCitationEntry();
        blobCitationEntry.setPosition(positionFn.apply(citationEntry));
        blobCitationEntry.setRawText(rawTextFn.apply(citationEntry));
        blobCitationEntry.setIdentifiers(identifiersFn.apply(citationEntry));
        return blobCitationEntry;
    }

    /**
     * Build position value of blob citation entry from citation entry.
     */
    public static class PositionBuilder {
        public static Integer build(CitationEntry citationEntry) {
            return citationEntry.getPosition();
        }
    }

    /**
     * Build raw text value of blob citation entry from citation entry.
     */
    public static class RawTextBuilder {
        public static String build(CitationEntry citationEntry) {
            return Optional.ofNullable(citationEntry.getRawText()).map(CharSequence::toString).orElse(null);
        }
    }

    /**
     * Build identifiers value of blob citation entry from citation entry.
     */
    public static class IdentifiersBuilder {
        public static List<TypedId> build(CitationEntry citationEntry) {
            return build(citationEntry,
                    TypedIdFromDestinationDocumentIdBuilder::isValid,
                    TypedIdFromDestinationDocumentIdBuilder::build,
                    TypedIdsFromExternalDestinationDocumentIdsBuilder::isValid,
                    TypedIdsFromExternalDestinationDocumentIdsBuilder::build);
        }

        public static List<TypedId> build(CitationEntry citationEntry,
                                          Function<CitationEntry, Boolean> destinationDocumentIdValidator,
                                          Function<CitationEntry, TypedId> typedIdFromDestinationDocumentIdFn,
                                          Function<CitationEntry, Boolean> externalDestinationDocumentIdsValidator,
                                          Function<CitationEntry, List<TypedId>> typedIdsFromExternalDestinationDocumentIdsFn) {
            List<TypedId> identifiers = new ArrayList<>();
            if (destinationDocumentIdValidator.apply(citationEntry)) {
                identifiers.add(typedIdFromDestinationDocumentIdFn.apply(citationEntry));
            }
            if (externalDestinationDocumentIdsValidator.apply(citationEntry)) {
                identifiers.addAll(typedIdsFromExternalDestinationDocumentIdsFn.apply(citationEntry));
            }
            return identifiers.isEmpty() ? null : identifiers;
        }

        public static class TypedIdFromDestinationDocumentIdBuilder {
            public static Boolean isValid(CitationEntry citationEntry) {
                return Objects.nonNull(citationEntry.getDestinationDocumentId());
            }

            public static TypedId build(CitationEntry citationEntry) {
                return build(citationEntry, TrustLevelBuilder::build);
            }

            public static TypedId build(CitationEntry citationEntry, Function<CitationEntry, Float> trustLevelFn) {
                return new TypedId(citationEntry.getDestinationDocumentId().toString(),
                        ExtraInfoConstants.CITATION_TYPE_OPENAIRE,
                        trustLevelFn.apply(citationEntry));
            }

            public static class TrustLevelBuilder {
                public static Float build(CitationEntry citationEntry) {
                    float confidenceLevel = citationEntry.getConfidenceLevel() != null ? citationEntry.getConfidenceLevel() : 1f;
                    return ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(confidenceLevel);
                }
            }
        }

        public static class TypedIdsFromExternalDestinationDocumentIdsBuilder {
            public static Boolean isValid(CitationEntry citationEntry) {
                return !citationEntry.getExternalDestinationDocumentIds().isEmpty();
            }

            public static List<TypedId> build(CitationEntry citationEntry) {
                float trustLevel = ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(1f);
                return citationEntry.getExternalDestinationDocumentIds().entrySet().stream()
                        .map(x -> new TypedId(x.getValue().toString(), x.getKey().toString(), trustLevel))
                        .collect(Collectors.toList());
            }
        }
    }
}
