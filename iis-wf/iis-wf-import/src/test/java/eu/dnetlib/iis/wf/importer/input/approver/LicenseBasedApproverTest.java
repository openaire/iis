package eu.dnetlib.iis.wf.importer.input.approver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.oaf.AccessRight;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
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
    public void testRejectForNullAccessRigh() {
        assertFalse(new LicenseBasedApprover("OPEN", null).approve(buildResultWithAccessRight(null)));
    }
    
    @Test
    public void testRejectForNullLicense() {
        assertFalse(new LicenseBasedApprover(null, "license").approve(buildResultWithLicense(null)));
    }
    
    @Test
    public void testRejectForIncompatibleAccessRight() {
        assertFalse(new LicenseBasedApprover("a^", null).approve(buildResultWithAccessRight("OPEN")));
        assertFalse(new LicenseBasedApprover("OPEN", null).approve(buildResultWithAccessRight("CLOSED")));
        assertFalse(new LicenseBasedApprover("OPEN", null).approve(buildResultWithAccessRight("OPEN SOURCE")));
    }
    
    @Test
    public void testRejectForIncompatibleLicense() {
        assertFalse(new LicenseBasedApprover(null, "a^").approve(
                buildResultWithLicense("http://creativecommons.org/licenses/by/4.0/")));
        assertFalse(new LicenseBasedApprover(null, "http://creativecommons.org/licenses/by/4.0/").approve(
                buildResultWithLicense("http://creativecommons.org/licenses/by/3.0/")));
        assertFalse(new LicenseBasedApprover(null, "http://creativecommons.org/licenses/by/4.0/").approve(
                buildResultWithLicense("http://creativecommons.org/licenses/by/4.0")));
    }

    @Test
    public void testAcceptForNullPatterns() {
        assertTrue(new LicenseBasedApprover(null, null).approve(buildResultWithAccessRightAndLicense("CLOSED", "some license")));
    }
    
    @Test
    public void testAcceptForNullAccessRightAndApprovedLicense() {
        assertTrue(new LicenseBasedApprover(null, "approved.*").approve(
                buildResultWithAccessRightAndLicense(null, "approved license")));
    }
    
    @Test
    public void testAcceptForApprovedAccessRightAndNullLicense() {
        assertTrue(new LicenseBasedApprover("OPEN.*", null).approve(
                buildResultWithAccessRightAndLicense("OPEN ACCESS", null)));
    }
    
    @Test
    public void testAcceptForCompliantAcessRightAndLicense() {
        assertTrue(new LicenseBasedApprover("^OPEN$", "http:\\/\\/creativecommons.org\\/licenses\\/.*").approve(
                buildResultWithAccessRightAndLicense("OPEN", "http://creativecommons.org/licenses/by/4.0/")));
        assertTrue(new LicenseBasedApprover("^OPEN.*", "http:\\/\\/creativecommons.org\\/licenses\\/by\\/4.0\\/").approve(
                buildResultWithAccessRightAndLicense("OPEN SOURCE", "http://creativecommons.org/licenses/by/4.0/")));
    }
    
    @Test
    public void testRejectForCompliantLicenseAndAccessRightButDifferentInstances() {
        assertFalse(new LicenseBasedApprover("^OPEN$", "http:\\/\\/creativecommons.org\\/licenses\\/.*").approve(
                buildResultWithAccessRightAndLicense("OPEN", "http://creativecommons.org/licenses/by/4.0/", false)));
    }
    
    // ------------------- PRIVATE --------------------------
    
    private Result buildResultWithAccessRight(String accessRightString) {
        return buildResultWithAccessRightAndLicense(accessRightString, null, true);
    }
    
    private Result buildResultWithLicense(String licenseString) {
        return buildResultWithAccessRightAndLicense(null, licenseString, true);
    }
    
    private Result buildResultWithAccessRightAndLicense(String accessRightString, String licenseString) {
        return buildResultWithAccessRightAndLicense(accessRightString, licenseString, true);
    }
    
    private Result buildResultWithAccessRightAndLicense(String accessRightString, String licenseString, 
            boolean shareInstance) {
        Result result = new Result();
        
        List<Instance> instances = new ArrayList<>();
        result.setInstance(instances);
        
        Instance instance = new Instance();
        instances.add(instance);
        
        if (StringUtils.isNotBlank(accessRightString)) {
            AccessRight accessRight = new AccessRight();
            accessRight.setClassid(accessRightString);
            instance.setAccessright(accessRight);
            
        }
        
        if (StringUtils.isNotBlank(licenseString)) {
            Field<String> license = new Field<>();
            license.setValue(licenseString);
            if (shareInstance) {
                instance.setLicense(license);    
            } else {
                Instance separateInstance = new Instance();
                separateInstance.setLicense(license);
                instances.add(separateInstance);
            }
            
        }
        
        return result;
    }
    
    
}
