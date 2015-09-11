package eu.dnetlib.iis.workflows.export.actionmanager.module;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.ExtraInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.statistics.schemas.ProjectToProjectStatistics;
import eu.dnetlib.iis.workflows.export.actionmanager.module.toxml.CommonCoreStatisticsXmlConverter;

/**
 * {@link ProjectToProjectStatistics} based action builder module.
 * @author mhorst
 *
 */
public class ProjectToProjectStatisticsActionBuilderModuleFactory 
	implements ActionBuilderFactory<ProjectToProjectStatistics> {

	private static final String EXTRA_INFO_NAME = ExtraInfoConstants.NAME_PROJECT_STATISTICS;
	private static final String EXTRA_INFO_TYPOLOGY = ExtraInfoConstants.TYPOLOGY_STATISTICS;

	private static final AlgorithmName algorithmName = AlgorithmName.project_statistics;
	
	class ProjectToProjectStatisticsActionBuilderModule extends AbstractBuilderModule 
	implements ActionBuilderModule<ProjectToProjectStatistics> {
		
		CommonCoreStatisticsXmlConverter converter = new CommonCoreStatisticsXmlConverter();
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public ProjectToProjectStatisticsActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
		
		@Override
		public List<AtomicAction> build(ProjectToProjectStatistics object, 
				Agent agent, String actionSetId) {
			Oaf oafObject = buildOAF(object);
			if (oafObject!=null) {
				return actionFactory.createUpdateActions(actionSetId,
								agent, object.getProjectId().toString(), 
								Type.project, oafObject.toByteArray());
			} else {
				return null;
			}
		}
		
		/**
		 * Builds OAF object.
		 * @param source
		 * @return OAF object
		 */
		protected Oaf buildOAF(ProjectToProjectStatistics source) {
			if (source.getStatistics()!=null) {
				OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getProjectId()!=null) {
					entityBuilder.setId(source.getProjectId().toString());	
				}
				ExtraInfo.Builder extraInfoBuilder = ExtraInfo.newBuilder();
				extraInfoBuilder.setValue(converter.serialize(source.getStatistics()));
				extraInfoBuilder.setName(EXTRA_INFO_NAME);
				extraInfoBuilder.setTypology(EXTRA_INFO_TYPOLOGY);
				extraInfoBuilder.setProvenance(this.inferenceProvenance);
				extraInfoBuilder.setTrust(getPredefinedTrust());
				entityBuilder.addExtraInfo(extraInfoBuilder.build());
				entityBuilder.setType(Type.project);
				return buildOaf(entityBuilder.build());
			}
	//		fallback
			return null;	
		}

	}


	@Override
	public ActionBuilderModule<ProjectToProjectStatistics> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new ProjectToProjectStatisticsActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}

}
