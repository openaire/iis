package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.util.regex.Pattern;

import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 * License data based result approver.
 * 
 * @author mhorst
 *
 */
public class LicenseBasedApprover implements ResultApprover {

    
    private static final long serialVersionUID = 2284855711266402117L;

    /**
     * Pattern describining access right to be whitelisted.
     * 
     */
    private final String accessRightWhitelistPattern;
    
    /**
     * Pattern describining licenses to be whitelisted.
     * 
     */
    private final String licenseWhitelistPattern;

    // ------------------------ CONSTRUCTORS --------------------------

    /**
     * @param accessRightWhitelistPattern regex pattern matching best access rights
     * @param licenseWhitelistPattern regex pattern matching license
     */
    public LicenseBasedApprover(String accessRightWhitelistPattern, String licenseWhitelistPattern) {
        this.accessRightWhitelistPattern = accessRightWhitelistPattern;
        this.licenseWhitelistPattern = licenseWhitelistPattern;
    }

    // ------------------------ LOGIC --------------------------

    @Override
    public boolean approve(Oaf oaf) {
        if (oaf != null) {
            if (oaf instanceof Result) {
                return approve((Result) oaf);
            } else {
                // if not result - accepting, just in case approver is used on a different Oaf
                // instance
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean approve(Result result) {
        for (Instance instance : result.getInstance()) {
            if (checkAccessRight(instance) && checkLicense(instance)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkAccessRight(Instance instance) {
        if (accessRightWhitelistPattern != null) {
            if (instance.getAccessright() != null && instance.getAccessright().getClassid() != null) {
                return Pattern.matches(accessRightWhitelistPattern, instance.getAccessright().getClassid());
            } else {
                // not accepting objects without specified access right if any pattern was declared
                return false;
            }
        } else {
            // access right restriction was not specified
            return true;
        }
    }
    
    private boolean checkLicense(Instance instance) {
        if (licenseWhitelistPattern != null) {
            if (instance.getLicense() != null && instance.getLicense().getValue() != null) {
                return Pattern.matches(licenseWhitelistPattern, instance.getLicense().getValue());
            } else {
                // not accepting objects without specified license if any pattern was declared
                return false;
            }
        } else {
            // license restriction was not specified
            return true;
        }
    }
    
}
