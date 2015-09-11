package eu.dnetlib.iis.workflows.export.actionmanager.module;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.ExtraInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.statistics.schemas.DocumentToDocumentStatistics;
import eu.dnetlib.iis.workflows.export.actionmanager.module.toxml.CommonBasicCitationStatisticsXmlConverter;

/**
 * {@link DocumentToDocumentStatistics} based action builder module.
 * @author mhorst
 *
 */
public class DocumentToDocumentStatisticsActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentToDocumentStatistics> {

	private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_RESULT_STATISTICS;
	private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_STATISTICS;
	
	private static final AlgorithmName algorithmName = AlgorithmName.document_statistics;
	
	class DocumentToDocumentStatisticsActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<DocumentToDocumentStatistics> {
	
		CommonBasicCitationStatisticsXmlConverter converter = new CommonBasicCitationStatisticsXmlConverter();
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public DocumentToDocumentStatisticsActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToDocumentStatistics object,
				Agent agent, String actionSetId) {
			Oaf oaf = buildOAFStatistics(object);
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
		protected Oaf buildOAFStatistics(DocumentToDocumentStatistics source) {
			if (source.getStatistics()!=null) {
				OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getDocumentId()!=null) {
					entityBuilder.setId(source.getDocumentId().toString());	
				}
				ExtraInfo.Builder extraInfoBuilder = ExtraInfo.newBuilder();
				extraInfoBuilder.setValue(converter.serialize(source.getStatistics()));
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
	}
	
		@Override
	public ActionBuilderModule<DocumentToDocumentStatistics> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentToDocumentStatisticsActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
		
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}

