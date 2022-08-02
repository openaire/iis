package eu.dnetlib.iis.wf.importer.infospace.approver;

import java.util.regex.Pattern;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 * Inference data based result approver.
 * 
 * @author mhorst
 *
 */
public class LicenseBasedApprover implements ResultApprover {

    
    private static final long serialVersionUID = 2284855711266402117L;

    /**
     * Pattern describining licenses to be whitelisted.
     * 
     */
    private final String licenseWhitelistPattern;

    // ------------------------ CONSTRUCTORS --------------------------

    /**
     * @param licenseWhitelistPattern regex pattern matching license
     */
    public LicenseBasedApprover(String licenseWhitelistPattern) {
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
        if (licenseWhitelistPattern != null) {
            if (result.getBestaccessright() != null && result.getBestaccessright().getClassid() != null) {
                return Pattern.matches(licenseWhitelistPattern, result.getBestaccessright().getClassid());
            } else {
                // not accepting Result objects without specified license if any pattern was
                // declared
                return false;
            }
        } else {
            return true;
        }
    }

}
