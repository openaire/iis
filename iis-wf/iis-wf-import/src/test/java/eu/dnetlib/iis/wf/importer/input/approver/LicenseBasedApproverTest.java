package eu.dnetlib.iis.wf.importer.input.approver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.wf.importer.infospace.approver.LicenseBasedApprover;

/**
 * {@link LicenseBasedApprover} test class.
 * 
 * @author mhorst
 *
 */
public class LicenseBasedApproverTest {

    // -------------------- TESTS --------------------------

    @Test
    public void testRejectForNullLicense() {
        assertFalse(new LicenseBasedApprover("OPEN").approve(buildResultWithLicense(null)));
    }
    
    @Test
    public void testRejectForIncompatibleLicense() {
        assertFalse(new LicenseBasedApprover("a^").approve(buildResultWithLicense("OPEN")));
        assertFalse(new LicenseBasedApprover("OPEN").approve(buildResultWithLicense("CLOSED")));
        assertFalse(new LicenseBasedApprover("OPEN").approve(buildResultWithLicense("OPEN SOURCE")));
    }

    @Test
    public void testAcceptForNullLicensePattern() {
        assertTrue(new LicenseBasedApprover(null).approve(buildResultWithLicense(null)));
    }
    
    @Test
    public void testAcceptForCompliantLicense() {
        assertTrue(new LicenseBasedApprover("^OPEN$").approve(buildResultWithLicense("OPEN")));
        assertTrue(new LicenseBasedApprover("^OPEN.*").approve(buildResultWithLicense("OPEN SOURCE")));
    }
    
    // ------------------- PRIVATE --------------------------
    
    private Result buildResultWithLicense(String license) {
        Result result = new Result();
        if (StringUtils.isNotBlank(license)) {
            Qualifier licenseQualifier = new Qualifier();
            licenseQualifier.setClassid(license);
            result.setBestaccessright(licenseQualifier);
        }
        return result;
    }
    
    
}
