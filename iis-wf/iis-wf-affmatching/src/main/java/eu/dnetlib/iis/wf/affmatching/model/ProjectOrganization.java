package eu.dnetlib.iis.wf.affmatching.model;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Paired project and organization identifiers.
 * 
 * @author mhorst
 *
 */
public class ProjectOrganization {

	private String projectId;

	private String organizationId;

	//------------------------ CONSTRUCTORS --------------------------
	
	public ProjectOrganization(String projecttId, String organizationId) {
		Preconditions.checkArgument(StringUtils.isNotBlank(projecttId));
		Preconditions.checkArgument(StringUtils.isNotBlank(organizationId));
		this.projectId = projecttId;
		this.organizationId = organizationId;
	}

	//------------------------ GETTERS --------------------------
	
	/**
	 * Project identifier.
	 */
	public String getProjectId() {
		return projectId;
	}

	/**
	 * Organization identifier.
	 */
	public String getOrganizationId() {
		return organizationId;
	}

	//------------------------ SETTERS --------------------------
	
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}

}
