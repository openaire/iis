package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

/**
 * Metadata extractected from funding tree.
 * @author mhorst
 *
 */
public class FundingDetails {
    
    private static final String FUNDER_FUNDING_SEPARATOR = "::";

    private final String funderShortName;
    
    private final List<CharSequence> fundingLevelNames;
    
    /**
     * @param funderShortName funder short name
     * @param fundingLevelNames funding level names indexed with level number
     */
    public FundingDetails(String funderShortName, List<CharSequence> fundingLevelNames) {
        this.funderShortName = funderShortName;
        this.fundingLevelNames = fundingLevelNames;
    }

    public String getFunderShortName() {
        return funderShortName;
    }

    public List<CharSequence> getFundingLevelNames() {
        return fundingLevelNames;
    }
    
    /**
     * @return funding class based of funder short name and level0 name, null returned when neither found.
     */
    public String buildFundingClass() {
        StringBuilder strBuilder = new StringBuilder();
        if (funderShortName!=null) {
            strBuilder.append(funderShortName);
            strBuilder.append(FUNDER_FUNDING_SEPARATOR);
            if (CollectionUtils.isNotEmpty(fundingLevelNames) && fundingLevelNames.get(0) != null) {
                strBuilder.append(fundingLevelNames.get(0));    
            }
            return strBuilder.toString();
        } else {
            if (CollectionUtils.isNotEmpty(fundingLevelNames) && fundingLevelNames.get(0) != null) {
                strBuilder.append(FUNDER_FUNDING_SEPARATOR);
                strBuilder.append(fundingLevelNames.get(0));
                return strBuilder.toString();
            } else {
                return null;
            }
        }
    }

}
