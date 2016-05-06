package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;

/**
 * Converter of {@link ProjectToOrganization} into {@link AffMatchProjectOrganization}
 * 
 * @author mhorst
 */

public class ProjectOrganizationConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    // ------------------------ LOGIC --------------------------

    /**
     * Converts {@link ProjectToOrganization} into {@link AffMatchProjectOrganization}
     */
    public AffMatchProjectOrganization convert(ProjectToOrganization projectOrganization) {
        Preconditions.checkNotNull(projectOrganization);
        Preconditions.checkArgument(StringUtils.isNotBlank(projectOrganization.getProjectId()));
        Preconditions.checkArgument(StringUtils.isNotBlank(projectOrganization.getOrganizationId()));
        return new AffMatchProjectOrganization(projectOrganization.getProjectId().toString(),
                projectOrganization.getOrganizationId().toString());
    }

}
