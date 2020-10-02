package eu.dnetlib.iis.wf.importer.input.approver;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DataInfoBasedApprover} test class.
 * 
 * @author mhorst
 *
 */
public class DataInfoBasedApproverTest {

    // -------------------- TESTS --------------------------
    
	@Test
	public void testRejectForInferenceProvenanceMatched() {
        assertFalse(new DataInfoBasedApprover("iis", true, null).approve(buildDataInfo("iis")));
        assertFalse(new DataInfoBasedApprover("iis::\\w*", true, null).approve(buildDataInfo("iis::some_alg")));
        assertFalse(new DataInfoBasedApprover("iis::.*", true, null).approve(buildDataInfo("iis::some_alg::version")));
	}
	
	@Test
    public void testAcceptForInferenceProvenanceUnmatched() {
        assertTrue(new DataInfoBasedApprover("iis::.*", true, null).approve(buildDataInfo("iis")));
        assertTrue(new DataInfoBasedApprover("iis::.*", true, null).approve(buildDataInfo("iis:test")));
	}
	
	@Test
    public void testAcceptNoBlacklistDefined() {
	    assertTrue(new DataInfoBasedApprover(null, false, null).approve(buildDataInfo(null)));
	    assertTrue(new DataInfoBasedApprover(null, false, null).approve(buildDataInfo("iis")));
	}
	
	@Test
    public void testAcceptNullOrEmptyInferenceProvenance() {
	    assertTrue(new DataInfoBasedApprover("iis", true, null).approve(buildDataInfo(null)));
	    assertTrue(new DataInfoBasedApprover("iis", true, null).approve(buildDataInfo("")));
	}
	
	@Test
    public void testAcceptNotInferred() {
        assertTrue(new DataInfoBasedApprover("iis::.*", true, null)
                .approve(buildDataInfo(false, false, "iis::some_alg::version")));
	}
	
	@Test
    public void testRejectInvisible() {
	    assertFalse(new DataInfoBasedApprover(null, false, null).approve(builInvisibledDataInfo()));
	    assertFalse(new DataInfoBasedApprover("iis", true, 0.5f).approve(builInvisibledDataInfo()));
	}

	@Test
    public void testRejectDeletedByInference() {
	    assertFalse(new DataInfoBasedApprover(null, true, null).approve(buildDataInfo(true, false, null)));
    }
	
	@Test
    public void testAcceptNullBooleansInDataInfo() {
	    assertTrue(new DataInfoBasedApprover("iis", true, null).approve(buildDataInfo(null, null, null)));
	}
	
	@Test
    public void testAcceptForValidTrustLevel() {
	    assertTrue(new DataInfoBasedApprover("iis", true, 0.5f).approve(buildDataInfoWithTrust("0.6")));
    }
	
	@Test
    public void testRejectForTrustLevelBelowThreshold() {
	    assertFalse(new DataInfoBasedApprover("iis", true, 0.5f).approve(buildDataInfoWithTrust("0.1")));
    }
	
	@Test
    public void testAcceptForInvalidTrustLevel() {
	    assertTrue(new DataInfoBasedApprover("iis", true, 0.5f).approve(buildDataInfoWithTrust("FULL")));
    }
	
	// -------------------- PRIVATE --------------------------
	
	private DataInfo buildDataInfo(String inferenceprovenance) {
        return buildDataInfo(false, true, inferenceprovenance);
    }
	
	private DataInfo buildDataInfoWithTrust(String trust) {
        return buildDataInfo(false, true, null, trust);
    }
	
	private DataInfo buildDataInfo(Boolean deletedbyinference, Boolean inferred, String inferenceprovenance) {
	    return buildDataInfo(deletedbyinference, inferred, inferenceprovenance, null);
	}
	
    private DataInfo buildDataInfo(Boolean deletedbyinference, Boolean inferred, String inferenceprovenance,
            String trust) {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setDeletedbyinference(deletedbyinference);
        dataInfo.setInferred(inferred);
        dataInfo.setInferenceprovenance(inferenceprovenance);
        dataInfo.setTrust(trust);
        return dataInfo;
    }
	
	private DataInfo builInvisibledDataInfo() {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInvisible(true);
        return dataInfo;
    }

}
