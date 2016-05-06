package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.ProjectOrganizationConverter;

public class ProjectOrganizationConverterTest {

    private ProjectOrganizationConverter converter = new ProjectOrganizationConverter();

    private final String projId = "projId";
    private final String orgId = "orgId";

    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void convert_null() {
        // execute
        converter.convert(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_organization_id() {
        // execute
        converter.convert(new ProjectToOrganization(projId, " "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_project_id() {
        // execute
        converter.convert(new ProjectToOrganization(" ", orgId));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_null_organization_id() {
        // execute
        converter.convert(new ProjectToOrganization(projId, null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_null_project_id() {
        // execute
        converter.convert(new ProjectToOrganization(null, orgId));
    }

    @Test
    public void convert() {
        // execute
        AffMatchProjectOrganization result = converter.convert(new ProjectToOrganization(projId, orgId));
        // assert
        assertEquals(projId, result.getProjectId());
        assertEquals(orgId, result.getOrganizationId());
    }

}
