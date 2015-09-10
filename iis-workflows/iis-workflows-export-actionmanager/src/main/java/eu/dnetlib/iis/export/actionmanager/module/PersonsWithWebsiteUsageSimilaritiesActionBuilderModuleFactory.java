package eu.dnetlib.iis.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.iis.websiteusage.schemas.PersonsWithWebsiteUsageSimilarities;


/**
 * {@link PersonsWithWebsiteUsageSimilarities} based action builder module.
 * @author mhorst
 *
 */
public class PersonsWithWebsiteUsageSimilaritiesActionBuilderModuleFactory 
	implements ActionBuilderFactory<PersonsWithWebsiteUsageSimilarities> {

	private static final AlgorithmName algorithmName = AlgorithmName.person_similarities_websiteusage;
	
	class PersonsWithWebsiteUsageSimilaritiesActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<PersonsWithWebsiteUsageSimilarities> {
		
		
		/**
		 * Default similarity level.
		 */
		private final float defaultSimilarityLevel = 0;
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public PersonsWithWebsiteUsageSimilaritiesActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(PersonsWithWebsiteUsageSimilarities object,
				Agent agent, String actionSetId) {
			return Arrays.asList(new AtomicAction[] {
					createSimilarityRelAction(actionSetId, agent, 
						object.getPersonId().toString(), 
						object.getOtherPersonId().toString(), 
						object.getSimilarities()!=null?
								object.getSimilarities().getCovisitedSimilarity():
									defaultSimilarityLevel),
					createSimilarityRelAction(actionSetId, agent, 
						object.getOtherPersonId().toString(), 
						object.getPersonId().toString(), 
						object.getSimilarities()!=null?
								object.getSimilarities().getCovisitedSimilarity():
									defaultSimilarityLevel)
			});
		}
	
		/**
		 * Creates atomic action representing similarity relation.
		 * @param actionSet
		 * @param agent
		 * @param personId
		 * @param otherPersonId
		 * @param similarityLevel
		 * @return atomic action representing similarity relation
		 */
		protected AtomicAction createSimilarityRelAction(String actionSet, Agent agent,
				String personId, String otherPersonId, float similarityLevel) {
			Oaf oafRel = buildOAFRel(personId, otherPersonId,
					similarityLevel);
			return actionFactory.createAtomicAction(
					actionSet, agent, 
					personId, 
					OafDecoder.decode(oafRel).getCFQ(), 
					otherPersonId, 
					oafRel.toByteArray());
		}
		
		/**
		 * Builds OAF object.
		 * @param source
		 * @param target
		 * @param score
		 * @return OAF object
		 */
		protected Oaf buildOAFRel(String source, String target, float score) {
			OafRel.Builder relBuilder = OafRel.newBuilder();
			relBuilder.setSource(source);
			relBuilder.setTarget(target);
			relBuilder.setChild(false);
			relBuilder.setRelType(RelType.personPerson);
			relBuilder.setSubRelType(SubRelType.similarity);
//			TODO shouldn't we have dedicated website usage similarity class?
//			relBuilder.setRelClass(value);
//			TODO we should set it in metadata field as classid 
//			PersonPerson.Builder personPersonBuilder = PersonPerson.newBuilder();
//			TODO currently we are unable to set personPerson similarity, no such field in model
//			personPersonBuilder.setSimilarity();
//			relBuilder.setSimilarityRel(
//					SimilarityRel.newBuilder().setType(
//							SimilarityRel.Type.WEBUSAGE).setSimilarity(
//							score).build());
			eu.dnetlib.data.proto.OafProtos.Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.setKind(Kind.relation);
			oafBuilder.setRel(relBuilder.build());
			oafBuilder.setDataInfo(buildInference());
			oafBuilder.setTimestamp(System.currentTimeMillis());
			return oafBuilder.build();
		}
	}
	
	@Override
	public ActionBuilderModule<PersonsWithWebsiteUsageSimilarities> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new PersonsWithWebsiteUsageSimilaritiesActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}

}
