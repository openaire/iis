package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;

/**
 * @author mhorst
 */

public class ProjectToOrganizationRelationConverterTest {

    ProjectToOrganizationRelationConverter converter = new ProjectToOrganizationRelationConverter();


    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void buildObject_null_oafRel() throws Exception {
        // execute
        converter.convert(null);
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