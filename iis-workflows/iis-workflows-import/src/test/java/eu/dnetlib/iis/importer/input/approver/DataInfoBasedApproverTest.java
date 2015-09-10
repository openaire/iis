package eu.dnetlib.iis.importer.input.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;

/**
 * {@link DataInfoBasedApprover} test class.
 * 
 * @author mhorst
 *
 */
public class DataInfoBasedApproverTest {

	@Test
	public void testMatching() throws Exception {
		assertFalse(new DataInfoBasedApprover("iis", true, null)
				.approve(DataInfo.newBuilder()
						.setProvenanceaction(Qualifier.newBuilder().build())
						.setDeletedbyinference(false).setInferred(true)
						.setInferenceprovenance("iis").build()));
		assertFalse(new DataInfoBasedApprover("iis::\\w*", true, null)
		.approve(DataInfo.newBuilder()
				.setProvenanceaction(Qualifier.newBuilder().build())
				.setDeletedbyinference(false).setInferred(true)
				.setInferenceprovenance("iis::some_alg").build()));
		assertFalse(new DataInfoBasedApprover("iis::.*", true, null)
		.approve(DataInfo.newBuilder()
				.setProvenanceaction(Qualifier.newBuilder().build())
				.setDeletedbyinference(false).setInferred(true)
				.setInferenceprovenance("iis::some_alg::version").build()));
//		approved: non inferenced
		assertTrue(new DataInfoBasedApprover("iis::.*", true, null)
		.approve(DataInfo.newBuilder()
				.setProvenanceaction(Qualifier.newBuilder().build())
				.setDeletedbyinference(false).setInferred(false)
				.setInferenceprovenance("iis::some_alg::version").build()));
//		approved: unmatched inferenceprovenance
		assertTrue(new DataInfoBasedApprover("iis::.*", true, null)
		.approve(DataInfo.newBuilder()
				.setProvenanceaction(Qualifier.newBuilder().build())
				.setDeletedbyinference(false).setInferred(true)
				.setInferenceprovenance("iis").build()));
		assertTrue(new DataInfoBasedApprover("iis::.*", true, null)
		.approve(DataInfo.newBuilder()
				.setProvenanceaction(Qualifier.newBuilder().build())
				.setDeletedbyinference(false).setInferred(true)
				.setInferenceprovenance("iis:test").build()));

	}

}
