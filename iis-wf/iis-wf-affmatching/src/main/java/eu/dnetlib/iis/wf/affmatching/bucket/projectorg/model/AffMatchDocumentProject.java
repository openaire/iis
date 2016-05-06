package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Paired document and project identifiers.
 * 
 * Confidence level describes the level of confidence given relation is valid.
 * 
 * @author mhorst
 *
 */
public class AffMatchDocumentProject {

    private String documentId;

    private String projectId;

    private float confidenceLevel;

    // ------------------------ CONSTRUCTORS --------------------------

    public AffMatchDocumentProject(String documentId, String projectId, float confidenceLevel) {
        Preconditions.checkArgument(StringUtils.isNotBlank(documentId));
        Preconditions.checkArgument(StringUtils.isNotBlank(projectId));
        Preconditions.checkArgument(confidenceLevel >= 0 && confidenceLevel <= 1);
        this.documentId = documentId;
        this.projectId = projectId;
        this.confidenceLevel = confidenceLevel;
    }

    // ------------------------ GETTERS --------------------------

    /**
     * Document identifier.
     */
    public String getDocumentId() {
        return documentId;
    }

    /**
     * Project identifier.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Document and project relation confidence level. Expressed as <0,1> range.
     */
    public float getConfidenceLevel() {
        return confidenceLevel;
    }

    // ------------------------ SETTERS --------------------------

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public void setConfidenceLevel(float confidenceLevel) {
        this.confidenceLevel = confidenceLevel;
    }

}
