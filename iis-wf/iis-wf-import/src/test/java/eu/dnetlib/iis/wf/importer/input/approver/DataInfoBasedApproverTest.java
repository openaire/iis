package eu.dnetlib.iis.wf.importer.input.approver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;

/**
 * {@link DataInfoBasedApprover} test class.
 * 
 * @author mhorst
 *
 */
public class DataInfoBasedApproverTest {

	@Test
	public void testMatching() throws Exception {
        assertFalse(new DataInfoBasedApprover("iis", true, null).approve(buildDataInfo(false, true, "iis")));
        assertFalse(new DataInfoBasedApprover("iis::\\w*", true, null)
                .approve(buildDataInfo(false, true, "iis::some_alg")));
        assertFalse(new DataInfoBasedApprover("iis::.*", true, null)
                .approve(buildDataInfo(false, true, "iis::some_alg::version")));
        // approved: non inferenced
        assertTrue(new DataInfoBasedApprover("iis::.*", true, null)
                .approve(buildDataInfo(false, false, "iis::some_alg::version")));
        // approved: unmatched inferenceprovenance
        assertTrue(new DataInfoBasedApprover("iis::.*", true, null).approve(buildDataInfo(false, true, "iis")));

        assertTrue(new DataInfoBasedApprover("iis::.*", true, null).approve(buildDataInfo(false, true, "iis:test")));

        // approved: no provenance blacklist defined
        assertTrue(new DataInfoBasedApprover(null, false, null).approve(buildDataInfo(false, false, null)));

        // not approved: invisible
        assertFalse(new DataInfoBasedApprover(null, false, null).approve(builInvisibledDataInfo()));
	}
	
	private DataInfo buildDataInfo(boolean deletedbyinference, boolean inferred, String inferenceprovenance) {
	    DataInfo dataInfo = new DataInfo();
	    dataInfo.setDeletedbyinference(deletedbyinference);
	    dataInfo.setInferred(inferred);
	    dataInfo.setInferenceprovenance(inferenceprovenance);
	    return dataInfo;
	}
	
	private DataInfo builInvisibledDataInfo() {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInvisible(true);
        return dataInfo;
    }

}
