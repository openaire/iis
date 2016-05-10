package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * Creates {@link AffMatchDocumentOrganization} objects based on {@link AffMatchDocumentProject}
 * and {@link AffMatchProjectOrganization} combined datastores.
 * 
 * @author mhorst
 *
 */
public class DocumentOrganizationCombiner implements Serializable {


    private static final long serialVersionUID = 1L;

    /**
     * Creates {@link AffMatchDocumentOrganization} relations.
     * 
     * @param docProjRDD {@link AffMatchDocumentProject} relations
     * @param projOrgRDD {@link AffMatchProjectOrganization} relations
     * @param docProjConfidenceLevelThreshold document project relation confidence level threshold,
     * confidence level check is skipped when this parameter is set to null
     */
    public JavaRDD<AffMatchDocumentOrganization> combine(JavaRDD<AffMatchDocumentProject> docProjRDD,
            JavaRDD<AffMatchProjectOrganization> projOrgRDD, Float docProjConfidenceLevelThreshold) {
        Preconditions.checkNotNull(docProjRDD);
        Preconditions.checkNotNull(projOrgRDD);
        JavaPairRDD<String, AffMatchDocumentProject> projIdToDocProj = docProjRDD
                .keyBy(docProj -> docProj.getProjectId())
                .filter(docProj -> (docProjConfidenceLevelThreshold == null || docProj._2.getConfidenceLevel() >= docProjConfidenceLevelThreshold));
        JavaPairRDD<String, AffMatchProjectOrganization> projIdToProjOrg = projOrgRDD
                .keyBy(projOrg -> projOrg.getProjectId());
        return projIdToDocProj.join(projIdToProjOrg).map(x -> {
            return new AffMatchDocumentOrganization(x._2._1.getDocumentId(), x._2._2.getOrganizationId());
        }).distinct();
    }
}
