package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
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
public class CitationsActionBuilderModuleFactory extends AbstractActionBuilderFactory<Citations, Result> {

    private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_CITATIONS;
    private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_CITATIONS;

    // ------------------------ CONSTRUCTORS --------------------------
    
    public CitationsActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedDocuments);
    }

    // ------------------------ LOGIC ---------------------------------
    
    @Override
    public ActionBuilderModule<Citations, Result> instantiate(Configuration config) {
        return new CitationActionBuilderModule(provideTrustLevelThreshold(config));
    }

    // ------------------------ INNER CLASS  --------------------------
    
    class CitationActionBuilderModule extends AbstractEntityBuilderModule<Citations, Result> {

        private CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();

        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public CitationActionBuilderModule(Float trustLevelThreshold) {
            super(trustLevelThreshold, buildInferenceProvenance());
        }

        // ------------------------ LOGIC --------------------------
        
        @Override
        protected Class<Result> getResultClass() {
            return Result.class;
        }
        
        @Override
        protected Result convert(Citations source) {
            if (CollectionUtils.isNotEmpty(source.getCitations())) {
                Result result = new Result();
                result.setId(source.getDocumentId().toString());
                
                ExtraInfo extraInfo = new ExtraInfo();
                extraInfo.setValue(converter.serialize(normalize(source.getCitations())));
                extraInfo.setName(EXTRA_INFO_NAME);
                extraInfo.setTypology(EXTRA_INFO_TYPOLOGY);
                extraInfo.setProvenance(this.getInferenceProvenance());
                extraInfo.setTrust(StaticConfigurationProvider.ACTION_TRUST_0_9);
                result.setExtraInfo(Arrays.asList(extraInfo));

                return result;
            } else {
                return null;    
            }
        }

        // ------------------------ PRIVATE --------------------------
        
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
                                        InfoSpaceConstants.ROW_PREFIX_SEPARATOR)[1]);
                    }
                    results.add(CitationsActionBuilderModuleUtils.build(currentEntry));
                }
                return results;
            } else {
                return null;
            }
        }
    }
}