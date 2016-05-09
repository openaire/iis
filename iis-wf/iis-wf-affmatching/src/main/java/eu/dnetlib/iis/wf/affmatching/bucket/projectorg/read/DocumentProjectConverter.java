package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * Converter of {@link DocumentToProject} into {@link AffMatchDocumentProject}
 * 
 * @author mhorst
 */

public class DocumentProjectConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    // ------------------------ LOGIC --------------------------

    /**
     * Converts {@link DocumentToProject} into {@link AffMatchDocumentProject}
     */
    public AffMatchDocumentProject convert(DocumentToProject documentProject) {
        Preconditions.checkNotNull(documentProject);
        Preconditions.checkArgument(StringUtils.isNotBlank(documentProject.getDocumentId()));
        Preconditions.checkArgument(StringUtils.isNotBlank(documentProject.getProjectId()));
        Preconditions.checkArgument(documentProject.getConfidenceLevel() != null
                && documentProject.getConfidenceLevel() >= 0 && documentProject.getConfidenceLevel() <= 1);
        return new AffMatchDocumentProject(documentProject.getDocumentId().toString(),
                documentProject.getProjectId().toString(), documentProject.getConfidenceLevel());
    }

}
