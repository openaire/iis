package eu.dnetlib.iis.wf.affmatching.bucket;

import java.util.List;

import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;

/**
 * An implementation of {@link BucketHasher} that hashes organization name string.
 * Hash is generated from 'main' (most significant) section of organization name.
 * 
 * @author madryk
 */
public class MainSectionBucketHasher implements BucketHasher<String> {

    private static final long serialVersionUID = 1L;


    private OrganizationSectionsSplitter sectionsSplitter = new OrganizationSectionsSplitter();
    
    private OrganizationSectionHasher sectionHasher = new OrganizationSectionHasher();
    
    
    private OrgSectionType mainSectionType = OrgSectionType.UNIVERSITY;
    
    private FallbackSectionPickStrategy  fallbackSectionPickStrategy = FallbackSectionPickStrategy.FIRST_SECTION;
    
    
    public enum FallbackSectionPickStrategy {
        FIRST_SECTION,
        LAST_SECTION
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed organization name string.<br/>
     * Hash is generated from main (most significant) section of organization name
     * using using {@link OrganizationSectionHasher}.<br/>
     * <br/>
     * Section is considered to be most significant when it is the first section
     * with type {@link #setMainSectionType(OrgSectionType)}
     * ({@link OrgSectionType#UNIVERSITY} by default).<br/>
     * <br/>
     * If no section meets this criteria then a fallback section will be taken.
     * Fallback section is taken based on {@link FallbackSectionPickStrategy}.
     * It is the first section if fallback strategy is {@link FallbackSectionPickStrategy#FIRST_SECTION}
     * or the last section if fallback strategy is {@link FallbackSectionPickStrategy#LAST_SECTION}.
     * First section is the default behavior.
     */
    public String hash(String organizationName) {
        
        List<OrganizationSection> sections = sectionsSplitter.splitToSectionsDetailed(organizationName);
        
        if (sections.isEmpty()) {
            return "";
        }
        
        
        OrganizationSection mainSection = pickMainSection(sections);
        
        return sectionHasher.hash((mainSection != null) ? mainSection : pickFallbackSection(sections));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private OrganizationSection pickMainSection(List<OrganizationSection> sections) {
        
        return sections.stream()
                .filter(x -> x.getType() == mainSectionType)
                .findFirst().orElse(null);
    }
    
    private OrganizationSection pickFallbackSection(List<OrganizationSection> sections) {
        
        switch (fallbackSectionPickStrategy) {
            
            case FIRST_SECTION: return sections.get(0);
            case LAST_SECTION:  return sections.get(sections.size() - 1);
            
            default: throw new UnsupportedOperationException(fallbackSectionPickStrategy.name() + " is unsupported FallbackSectionPickStrategy");
        }
        
    }
    
    
    //------------------------ SETTERS --------------------------
    
    public void setSectionsSplitter(OrganizationSectionsSplitter sectionsSplitter) {
        this.sectionsSplitter = sectionsSplitter;
    }

    public void setSectionHasher(OrganizationSectionHasher sectionHasher) {
        this.sectionHasher = sectionHasher;
    }

    public void setMainSectionType(OrgSectionType mainSectionType) {
        this.mainSectionType = mainSectionType;
    }

    public void setFallbackSectionPickStrategy(FallbackSectionPickStrategy fallbackSectionPickStrategy) {
        this.fallbackSectionPickStrategy = fallbackSectionPickStrategy;
    }
}
