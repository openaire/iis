package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Map.Entry;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import eu.dnetlib.iis.export.schemas.Citations;

/**
 * {@link Citations} action builder module utilities.
 * @author mhorst
 *
 */
public final class CitationsActionBuilderModuleUtils {
    
    // ------------------------- CONSTRUCTORS ----------------------------
    
    private CitationsActionBuilderModuleUtils() {}

    // ------------------------- LOGIC -----------------------------------
    
    
    /**
     * Creates {@link BlobCitationEntry} from {@link CitationEntry}.
     * Translates confirence level into trust level applying confidenceToTrustLevelFactor.
     */
    public static BlobCitationEntry build(CitationEntry entry) {
        BlobCitationEntry result = new BlobCitationEntry(
                entry.getRawText() != null ? entry.getRawText().toString() : null);
        result.setPosition(entry.getPosition());
        if (entry.getDestinationDocumentId() != null) {
            result.setIdentifiers(new ArrayList<TypedId>());
            result.getIdentifiers()
                    .add(new TypedId(entry.getDestinationDocumentId().toString(),
                            ExtraInfoConstants.CITATION_TYPE_OPENAIRE,
                            entry.getConfidenceLevel() != null
                                    ? (entry.getConfidenceLevel() * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR)
                                    : 1f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
        }
        if (entry.getExternalDestinationDocumentIds() != null && !entry.getExternalDestinationDocumentIds().isEmpty()) {
            if (result.getIdentifiers() == null) {
                result.setIdentifiers(new ArrayList<TypedId>());
            }
            for (Entry<CharSequence, CharSequence> extId : entry.getExternalDestinationDocumentIds().entrySet()) {
                result.getIdentifiers().add(new TypedId(extId.getValue().toString(), extId.getKey().toString(),
                        1f * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR));
            }
        }
        return result;
    }
    
}
