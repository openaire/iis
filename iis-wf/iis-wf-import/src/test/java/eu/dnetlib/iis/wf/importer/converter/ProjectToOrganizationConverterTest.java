package eu.dnetlib.iis.wf.importer.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.UnsignedBytes;

import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization.Participation;
import eu.dnetlib.data.proto.RelMetadataProtos.RelMetadata;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * @author mhorst
 */

public class ProjectToOrganizationConverterTest {

	private byte[] relationColumnFamilyBytes = "dummy".getBytes();

	private Oaf oaf = null;

	private Result result = mock(Result.class);

	private ResultApprover defaultApprover = mock(ResultApprover.class);

	@Before
	public void setUp() {
		when(defaultApprover.approveBeforeBuilding(any())).thenReturn(true);
	}

	// ------------------------ TESTS --------------------------

	@Test(expected = NullPointerException.class)
	public void constructor_relationColumnFamilyBytes_NULL() throws Exception {
		// execute
		new ProjectToOrganizationConverter(defaultApprover, null);
	}

	@Test(expected = NullPointerException.class)
	public void constructor_resultApprover_NULL() throws Exception {
		// execute
		new ProjectToOrganizationConverter(null, relationColumnFamilyBytes);
	}

	@Test(expected = NullPointerException.class)
	public void buildObject_hbaseResult_NULL() throws Exception {
		// given
		ProjectToOrganizationConverter converter = new ProjectToOrganizationConverter(defaultApprover,
				relationColumnFamilyBytes);
		// execute
		converter.buildObject(null, oaf);
	}

	@Test
	public void buildObject_disapproved_candidates() throws Exception {
		// given
		when(defaultApprover.approveBeforeBuilding(any())).thenReturn(false);
		ProjectToOrganizationConverter converter = new ProjectToOrganizationConverter(defaultApprover,
				relationColumnFamilyBytes);

		String projectId = "someProjectId";
		String organizationId = "someOrgId";

		NavigableMap<byte[], byte[]> navigableMap = new TreeMap<byte[], byte[]>(
				UnsignedBytes.lexicographicalComparator());
		navigableMap.put(organizationId.getBytes(), createOafObject(projectId, organizationId).toByteArray());
		when(result.getFamilyMap(relationColumnFamilyBytes)).thenReturn(navigableMap);

		// execute
		Collection<ProjectToOrganization> projOrgs = converter.buildObject(result, oaf);

		// assert
		assertNotNull(projOrgs);
		assertTrue(projOrgs.isEmpty());
	}

	@Test
	public void buildObject_no_candidates() throws Exception {
		// given
		ProjectToOrganizationConverter converter = new ProjectToOrganizationConverter(defaultApprover,
				relationColumnFamilyBytes);

		when(result.getFamilyMap(relationColumnFamilyBytes))
				.thenReturn(new TreeMap<byte[], byte[]>(UnsignedBytes.lexicographicalComparator()));

		// execute
		Collection<ProjectToOrganization> projOrgs = converter.buildObject(result, oaf);

		// assert
		assertNotNull(projOrgs);
		assertTrue(projOrgs.isEmpty());
	}

	@Test
	public void buildObject() throws Exception {
		// given
		ProjectToOrganizationConverter converter = new ProjectToOrganizationConverter(defaultApprover,
				relationColumnFamilyBytes);

		String projectId = "someProjectId";
		String organization1Id = "someOrg1Id";
		String organization2Id = "someOrg2Id";

		NavigableMap<byte[], byte[]> navigableMap = new TreeMap<byte[], byte[]>(
				UnsignedBytes.lexicographicalComparator());
		navigableMap.put(organization1Id.getBytes(), createOafObject(projectId, organization1Id).toByteArray());
		navigableMap.put(organization2Id.getBytes(), createOafObject(projectId, organization2Id).toByteArray());
		when(result.getFamilyMap(relationColumnFamilyBytes)).thenReturn(navigableMap);

		// execute
		List<ProjectToOrganization> projOrgs = converter.buildObject(result, oaf);

		// assert
		assertNotNull(projOrgs);
		assertEquals(2, projOrgs.size());
		assertNotNull(projOrgs.get(0));
		assertEquals(projectId, projOrgs.get(0).getProjectId());
		assertEquals(organization1Id, projOrgs.get(0).getOrganizationId());
		assertNotNull(projOrgs.get(1));
		assertEquals(projectId, projOrgs.get(1).getProjectId());
		assertEquals(organization2Id, projOrgs.get(1).getOrganizationId());
	}

	// ------------------------ PRIVATE --------------------------

	private Oaf createOafObject(String projectId, String organizationId) {
		String relClass = "hasParticipant";
		Qualifier semantics = Qualifier.newBuilder().setClassid(relClass).setClassname(relClass)
				.setSchemeid("dnet:project_organization_relations").setSchemename("dnet:project_organization_relations")
				.build();
		RelMetadata relMetadata = RelMetadata.newBuilder().setSemantics(semantics).build();
		OafRel rel = OafRel.newBuilder().setRelType(RelType.projectOrganization).setSubRelType(SubRelType.participation)
				.setRelClass(relClass).setChild(false).setSource(projectId).setTarget(organizationId)
				.setProjectOrganization(ProjectOrganization.newBuilder()
						.setParticipation(Participation.newBuilder().setRelMetadata(relMetadata)).build())
				.build();
		return Oaf.newBuilder().setKind(Kind.relation).setRel(rel).build();
	}

}