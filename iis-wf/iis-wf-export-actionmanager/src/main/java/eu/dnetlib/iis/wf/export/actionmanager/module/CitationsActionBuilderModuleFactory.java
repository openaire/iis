package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.ExtraInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link Citations} based action builder module.
 * 
 * @author mhorst
 *
 */
public class CitationsActionBuilderModuleFactory extends AbstractActionBuilderFactory<Citations> {

    private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_CITATIONS;
    private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_CITATIONS;

    // ------------------------ CONSTRUCTORS --------------------------
    
    public CitationsActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedDocuments);
    }

    // ------------------------ LOGIC ---------------------------------
    
    @Override
    public ActionBuilderModule<Citations> instantiate(Configuration config, Agent agent, String actionSetId) {
        return new CitationActionBuilderModule(provideTrustLevelThreshold(config), agent, actionSetId);
    }

    // ------------------------ INNER CLASS  --------------------------
    
    class CitationActionBuilderModule extends AbstractBuilderModule<Citations> {

        CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();

        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public CitationActionBuilderModule(Float trustLevelThreshold, Agent agent, String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), agent, actionSetId);
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        public List<AtomicAction> build(Citations object) {
            Oaf oaf = buildOAFCitations(object);
            if (oaf != null) {
                return actionFactory.createUpdateActions(actionSetId, agent, object.getDocumentId().toString(),
                        Type.result, oaf.toByteArray());
            } else {
                return Collections.emptyList();
            }
        }

        // ------------------------ PRIVATE --------------------------
        
        /**
         * Builds {@link Oaf} object containing document statistics.
         */
        private Oaf buildOAFCitations(Citations source) {
            if (CollectionUtils.isNotEmpty(source.getCitations())) {
                OafEntity.Builder entityBuilder = OafEntity.newBuilder();
                if (source.getDocumentId() != null) {
                    entityBuilder.setId(source.getDocumentId().toString());
                }
                ExtraInfo.Builder extraInfoBuilder = ExtraInfo.newBuilder();
                extraInfoBuilder.setValue(converter.serialize(normalize(source.getCitations())));
                extraInfoBuilder.setName(EXTRA_INFO_NAME);
                extraInfoBuilder.setTypology(EXTRA_INFO_TYPOLOGY);
                extraInfoBuilder.setProvenance(this.inferenceProvenance);
                extraInfoBuilder.setTrust(StaticConfigurationProvider.ACTION_TRUST_0_9);
                entityBuilder.addExtraInfo(extraInfoBuilder.build());
                entityBuilder.setType(Type.result);
                return buildOaf(entityBuilder.build());
            }
            // fallback
            return null;
        }

        /**
         * Performs confidence level normalization. Removes empty lists. 
         * Removes 50| prefix from publication identifier.
         * 
         * @param source list of citations to be normalized
         * @return {@link BlobCitationEntry} objects having confidence level value normalized
         */
        private SortedSet<BlobCitationEntry> normalize(List<CitationEntry> source) {
            if (source != null) {
                SortedSet<BlobCitationEntry> results = new TreeSet<BlobCitationEntry>();
                for (CitationEntry currentEntry : source) {
                    if (currentEntry.getExternalDestinationDocumentIds().isEmpty()) {
                        currentEntry.setExternalDestinationDocumentIds(null);
                    }
                    if (currentEntry.getDestinationDocumentId() != null) {
                        currentEntry.setDestinationDocumentId(
                                StringUtils.split(currentEntry.getDestinationDocumentId().toString(),
                                        HBaseConstants.ROW_PREFIX_SEPARATOR)[1]);
                    }
                    results.add(CitationsActionBuilderModuleUtils.build(currentEntry,
                            getConfidenceToTrustLevelNormalizationFactor()));
                }
                return results;
            } else {
                return null;
            }
        }
    }
}