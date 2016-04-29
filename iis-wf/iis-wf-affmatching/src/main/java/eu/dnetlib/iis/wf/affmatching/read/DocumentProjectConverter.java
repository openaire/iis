package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.model.DocumentProject;

/**
 * Converter of {@link DocumentToProject} into {@link DocumentProject}
 * 
 * @author mhorst
 */

public class DocumentProjectConverter implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------ LOGIC --------------------------

	/**
	 * Converts {@link DocumentToProject} into {@link DocumentProject}
	 */
	public DocumentProject convert(DocumentToProject documentProject) {
		Preconditions.checkNotNull(documentProject);
		Preconditions.checkArgument(StringUtils.isNotBlank(documentProject.getDocumentId()));
		Preconditions.checkArgument(StringUtils.isNotBlank(documentProject.getProjectId()));
		Preconditions.checkArgument(documentProject.getConfidenceLevel() != null
				&& documentProject.getConfidenceLevel() >= 0 && documentProject.getConfidenceLevel() <= 1);
		return new DocumentProject(documentProject.getDocumentId().toString(),
				documentProject.getProjectId().toString(), documentProject.getConfidenceLevel());
	}

}
