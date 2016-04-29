package eu.dnetlib.iis.wf.affmatching.hint;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.wf.affmatching.model.DocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;
import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;
import scala.Tuple2;

/**
 * Creates {@link DocumentOrganization} objects from document project and
 * project organization datastores.
 * 
 * @author mhorst
 *
 */
public class DocumentOrganizationBuilder implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates {@link DocumentOrganization} relations.
	 * 
	 * @param docProjRDD {@link DocumentProject} relations
	 * @param projOrgRDD {@link ProjectOrganization} relations
	 * @param docProjConfidenceLevelThreshold document project relation confidence level threshold
	 */
	public JavaRDD<DocumentOrganization> build(JavaRDD<DocumentProject> docProjRDD,
			JavaRDD<ProjectOrganization> projOrgRDD, Float docProjConfidenceLevelThreshold) {
		JavaPairRDD<String, DocumentProject> projIdToDocProj = docProjRDD
				.mapToPair(docProj -> new Tuple2<String, DocumentProject>(docProj.getProjectId(), docProj))
				.filter(docProj -> (docProjConfidenceLevelThreshold == null
						|| docProj._2.getConfidenceLevel() >= docProjConfidenceLevelThreshold));
		JavaPairRDD<String, ProjectOrganization> projIdToProjOrg = projOrgRDD
				.mapToPair(projOrg -> new Tuple2<String, ProjectOrganization>(projOrg.getProjectId(), projOrg));
		return projIdToDocProj.join(projIdToProjOrg).map(x -> {
			return new DocumentOrganization(x._2._1.getDocumentId(), x._2._2.getOrganizationId());
		}).distinct();
	}
}
