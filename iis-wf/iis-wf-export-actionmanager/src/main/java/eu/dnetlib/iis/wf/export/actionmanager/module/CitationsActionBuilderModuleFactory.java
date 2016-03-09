package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

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
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;
import eu.dnetlib.iis.export.schemas.Citations;


/**
 * {@link Citations} based action builder module.
 * @author mhorst
 *
 */
public class CitationsActionBuilderModuleFactory 
		implements ActionBuilderFactory<Citations> {
	
	private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_CITATIONS;
	private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_CITATIONS;
	
	private static final AlgorithmName algorithmName = AlgorithmName.document_referencedDocuments;
	
	class CitationActionBuilderModule extends AbstractBuilderModule
	implements ActionBuilderModule<Citations> {
	
		CitationsExtraInfoConverter converter = new CitationsExtraInfoConverter();
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public CitationActionBuilderModule(String predefinedTrust, 
				Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(Citations object, Agent agent, String actionSetId) {
			Oaf oaf = buildOAFCitations(object);
			if (oaf!=null) {
				return actionFactory.createUpdateActions(
						actionSetId,
						agent, object.getDocumentId().toString(), Type.result, 
						oaf.toByteArray());	
			} else {
				return Collections.emptyList();
			}
		}
		
		/**
		 * Builds OAF object containing document statistics.
		 * @param source
		 * @return OAF object containing document statistics
		 */
		protected Oaf buildOAFCitations(Citations source) {
			if (source.getCitations()!=null && source.getCitations().size()>0) {
				OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getDocumentId()!=null) {
					entityBuilder.setId(source.getDocumentId().toString());	
				}
				ExtraInfo.Builder extraInfoBuilder = ExtraInfo.newBuilder();
				extraInfoBuilder.setValue(converter.serialize(
						normalize(source.getCitations())));
				extraInfoBuilder.setName(EXTRA_INFO_NAME);
				extraInfoBuilder.setTypology(EXTRA_INFO_TYPOLOGY);
				extraInfoBuilder.setProvenance(this.inferenceProvenance);
				extraInfoBuilder.setTrust(getPredefinedTrust());
				entityBuilder.addExtraInfo(extraInfoBuilder.build());
				entityBuilder.setType(Type.result);
				return buildOaf(entityBuilder.build());
			}
//			fallback
			return null;
		}
		
		/**
		 * Performs confidence level normalization. Removes empty lists.
		 * Removes 50| prefix from publication identifier.
		 * @param source
		 * @return {@link BlobCitationEntry} objects having confidence level value normalized.
		 */
		private SortedSet<BlobCitationEntry> normalize(List<CitationEntry> source) {
			if (source!=null) {
				SortedSet<BlobCitationEntry> results = new TreeSet<BlobCitationEntry>();
				for (CitationEntry currentEntry : source) {
					if (currentEntry.getExternalDestinationDocumentIds().isEmpty()) {
						currentEntry.setExternalDestinationDocumentIds(null);
					}
					if (currentEntry.getDestinationDocumentId()!=null) {
						currentEntry.setDestinationDocumentId(
								StringUtils.split(currentEntry.getDestinationDocumentId().toString(), 
										HBaseConstants.ROW_PREFIX_SEPARATOR)[1]);
					}
					results.add(CitationsActionBuilderModuleFactory.build(
							currentEntry, getConfidenceToTrustLevelNormalizationFactor()));
				}
				return results;
			} else {
				return null;
			}
		}
	}

	@Override
	public ActionBuilderModule<Citations> instantiate(String predefinedTrust,
			Float trustLevelThreshold, Configuration config) {
		return new CitationActionBuilderModule(predefinedTrust, trustLevelThreshold);
	}
	
	public static BlobCitationEntry build(CitationEntry entry, float confidenceToTrustLevelFactor) {
		BlobCitationEntry result = new BlobCitationEntry(
				entry.getRawText()!=null?entry.getRawText().toString():null);
		result.setPosition(entry.getPosition());
		if (entry.getDestinationDocumentId()!=null) {
			result.setIdentifiers(new ArrayList<TypedId>());
			result.getIdentifiers().add(new TypedId(
					entry.getDestinationDocumentId().toString(),
					ExtraInfoConstants.CITATION_TYPE_OPENAIRE,
					entry.getConfidenceLevel()!=null?
							(entry.getConfidenceLevel()*confidenceToTrustLevelFactor):
								1f*confidenceToTrustLevelFactor));
		}
		if (entry.getExternalDestinationDocumentIds()!=null &&
				!entry.getExternalDestinationDocumentIds().isEmpty()) {
			if (result.getIdentifiers()==null) {
				result.setIdentifiers(new ArrayList<TypedId>());	
			}
			for (Entry<CharSequence, CharSequence> extId : entry.getExternalDestinationDocumentIds().entrySet()) {
				result.getIdentifiers().add(new TypedId(
						extId.getValue().toString(),
						extId.getKey().toString(),
						1f*confidenceToTrustLevelFactor));
			}
		}
		return result;
	}

	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
