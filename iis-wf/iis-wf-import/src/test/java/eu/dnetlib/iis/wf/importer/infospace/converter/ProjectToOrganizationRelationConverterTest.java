package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 */

public class ProjectToOrganizationRelationConverterTest {

    ProjectToOrganizationRelationConverter converter = new ProjectToOrganizationRelationConverter();


    // ------------------------ TESTS --------------------------

    @Test
    public void buildObject_null_oafRel() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }

    @Test
    public void buildObject() throws Exception {
        // given
        String projectId = "someProjectId";
        String organizationId = "someOrgId";

        // execute
        ProjectToOrganization projOrg = converter.convert(createOafRelObject(projectId, organizationId));

        // assert
        assertNotNull(projOrg);
        assertEquals(projectId, projOrg.getProjectId());
        assertEquals(organizationId, projOrg.getOrganizationId());

    }

    // ------------------------ PRIVATE --------------------------

    private Relation createOafRelObject(String projectId, String organizationId) {

        Relation relation = new Relation();
        relation.setRelType("projectOrganization");
        relation.setSubRelType("participation");
        relation.setRelClass("hasParticipant");
        relation.setSource(projectId);
        relation.setTarget(organizationId);
        
        return relation;
    }

}