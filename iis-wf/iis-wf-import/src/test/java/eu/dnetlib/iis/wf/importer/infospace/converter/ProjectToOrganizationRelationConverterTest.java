package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization.Participation;
import eu.dnetlib.data.proto.RelMetadataProtos.RelMetadata;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectToOrganizationRelationConverter;

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

    private OafRel createOafRelObject(String projectId, String organizationId) {
        String relClass = "hasParticipant";
        Qualifier semantics = Qualifier.newBuilder().setClassid(relClass).setClassname(relClass)
                .setSchemeid("dnet:project_organization_relations").setSchemename("dnet:project_organization_relations")
                .build();
        RelMetadata relMetadata = RelMetadata.newBuilder().setSemantics(semantics).build();
        return OafRel.newBuilder().setRelType(RelType.projectOrganization).setSubRelType(SubRelType.participation)
                .setRelClass(relClass).setChild(false).setSource(projectId).setTarget(organizationId)
                .setProjectOrganization(ProjectOrganization.newBuilder()
                        .setParticipation(Participation.newBuilder().setRelMetadata(relMetadata)).build())
                .build();
    }

}