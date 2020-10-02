package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProjectOrganizationConverterTest {

    private ProjectOrganizationConverter converter = new ProjectOrganizationConverter();

    private final String projId = "projId";
    private final String orgId = "orgId";

    // ------------------------ TESTS --------------------------

    @Test
    public void convert_null() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }

    @Test
    public void convert_blank_organization_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new ProjectToOrganization(projId, " ")));
    }

    @Test
    public void convert_blank_project_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new ProjectToOrganization(" ", orgId)));
    }

    @Test
    public void convert_null_organization_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new ProjectToOrganization(projId, null)));
    }

    @Test
    public void convert_null_project_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () -> converter.convert(new ProjectToOrganization(null, orgId)));
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
