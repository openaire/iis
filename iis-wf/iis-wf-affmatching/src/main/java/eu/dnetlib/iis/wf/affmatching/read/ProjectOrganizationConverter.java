package eu.dnetlib.iis.wf.affmatching.read;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;

/**
 * Converter of {@link ProjectToOrganization} into {@link ProjectOrganization}
 * 
 * @author mhorst
 */

public class ProjectOrganizationConverter implements Serializable {

	private static final long serialVersionUID = 1L;

	// ------------------------ LOGIC --------------------------

	/**
	 * Converts {@link ProjectToOrganization} into {@link ProjectOrganization}
	 */
	public ProjectOrganization convert(ProjectToOrganization projectOrganization) {
		Preconditions.checkNotNull(projectOrganization);
		Preconditions.checkArgument(StringUtils.isNotBlank(projectOrganization.getProjectId()));
		Preconditions.checkArgument(StringUtils.isNotBlank(projectOrganization.getOrganizationId()));
		return new ProjectOrganization(projectOrganization.getProjectId().toString(),
				projectOrganization.getOrganizationId().toString());
	}

}
