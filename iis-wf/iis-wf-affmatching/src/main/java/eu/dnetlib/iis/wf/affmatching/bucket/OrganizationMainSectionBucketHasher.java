package eu.dnetlib.iis.wf.affmatching.bucket;

import java.util.List;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;

/**
 * An implementation of {@link BucketHasher} that hashes {@link AffMatchOrganization}.
 * Hash is generated from 'main' (most significant) section of {@link AffMatchOrganization#getName()}.
 * 
 * @author madryk
 */
public class OrganizationMainSectionBucketHasher implements BucketHasher<AffMatchOrganization> {
    
    private static final long serialVersionUID = 1L;
    
    
    private OrganizationSectionsSplitter sectionsSplitter = new OrganizationSectionsSplitter();
    
    private OrganizationSectionHasher sectionHasher = new OrganizationSectionHasher();
    
    private OrgSectionType mainSectionType = OrgSectionType.UNIVERSITY;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed {@link AffMatchOrganization}.<br/>
     * Hash is generated from main (most significant) section of 
     * {@link AffMatchOrganization#getName()} using {@link OrganizationSectionHasher}.<br/>
     * 
     * Section is considered to be most significant when it is the first section
     * with type {@link #setMainSectionType(OrgSectionType)} 
     * ({@link OrgSectionType#UNIVERSITY} by default).<br/>
     * 
     * If no section meets this criteria then first section will be taken as a fallback.
     */
    @Override
    public String hash(AffMatchOrganization organization) {
        
        List<OrganizationSection> sections = sectionsSplitter.splitToSectionsDetailed(organization.getName());
        
        if (sections.isEmpty()) {
            return "";
        }
        
        
        OrganizationSection mainSection = pickMainSection(sections);
        
        if (mainSection != null) {
            return sectionHasher.hash(mainSection);
        }
        
        
        return sectionHasher.hash(pickFallbackSection(sections));
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private OrganizationSection pickMainSection(List<OrganizationSection> sections) {
        
        return sections.stream()
                .filter(x -> x.getType() == mainSectionType)
                .findFirst().orElse(null);
    }
    
    private OrganizationSection pickFallbackSection(List<OrganizationSection> sections) {
        return sections.get(0);
    }


    //------------------------ SETTERS --------------------------
    
    public void setMainSectionType(OrgSectionType mainSectionType) {
        this.mainSectionType = mainSectionType;
    }

}
