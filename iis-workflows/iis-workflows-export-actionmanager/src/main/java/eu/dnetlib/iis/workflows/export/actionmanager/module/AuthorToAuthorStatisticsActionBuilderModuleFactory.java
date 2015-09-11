package eu.dnetlib.iis.workflows.export.actionmanager.module;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.ExtraInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.statistics.schemas.AuthorStatistics;
import eu.dnetlib.iis.statistics.schemas.AuthorToAuthorStatistics;
import eu.dnetlib.iis.statistics.schemas.CoAuthor;
import eu.dnetlib.iis.workflows.export.actionmanager.module.toxml.AuthorStatisticsXmlConverter;

/**
 * {@link AuthorToAuthorStatistics} based action builder module.
 * @author mhorst
 *
 */
public class AuthorToAuthorStatisticsActionBuilderModuleFactory 
	implements ActionBuilderFactory<AuthorToAuthorStatistics> {

	private static final AlgorithmName algorithmName = AlgorithmName.person_statistics;
	
	private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_AUTHOR_STATISTICS;
	private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_STATISTICS;
	
	class AuthorToAuthorStatisticsActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<AuthorToAuthorStatistics> {
	
		AuthorStatisticsXmlConverter converter = new AuthorStatisticsXmlConverter();
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		private AuthorToAuthorStatisticsActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(AuthorToAuthorStatistics object, 
				Agent agent, String actionSetId) {
			Oaf oafObject = buildOAF(object);
			if (oafObject!=null) {
				return actionFactory.createUpdateActions(actionSetId,
								agent, object.getAuthorId().toString(), 
								Type.person, oafObject.toByteArray());
				
			} else {
				return Collections.emptyList();
			}
		}
	
		/**
		 * Builds OAF object.
		 * @param source
		 * @return OAF object
		 */
		protected Oaf buildOAF(AuthorToAuthorStatistics source) {
			if (source.getStatistics()!=null) {
				OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getAuthorId()!=null) {
					entityBuilder.setId(source.getAuthorId().toString());	
				}
				ExtraInfo.Builder extraInfoBuilder = ExtraInfo.newBuilder();
				extraInfoBuilder.setValue(converter.serialize(
						normalize(source.getStatistics())));
				extraInfoBuilder.setName(EXTRA_INFO_NAME);
				extraInfoBuilder.setTypology(EXTRA_INFO_TYPOLOGY);
				extraInfoBuilder.setProvenance(this.inferenceProvenance);
				extraInfoBuilder.setTrust(getPredefinedTrust());
				entityBuilder.addExtraInfo(extraInfoBuilder.build());
				entityBuilder.setType(Type.person);
				return buildOaf(entityBuilder.build());
			}
	//		fallback
			return null;	
		}
	}
	
	/**
	 * Performs normalization by removing 30| prefix from author identifier.
	 * @param source
	 * @return normalized {@link AuthorStatistics} object
	 */
	private AuthorStatistics normalize(AuthorStatistics source) {
		if (source!=null) {
			if (source.getCoAuthors()!=null && source.getCoAuthors().size()>0) {
				for (CoAuthor currentCoAuthor : source.getCoAuthors()) {
					if (currentCoAuthor.getId()!=null) {
						currentCoAuthor.setId(
								StringUtils.split(currentCoAuthor.getId().toString(), 
								HBaseConstants.ROW_PREFIX_SEPARATOR)[1]);
					}
				}
			}
		}
		return source;
	}

	@Override
	public ActionBuilderModule<AuthorToAuthorStatistics> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new AuthorToAuthorStatisticsActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}

}
