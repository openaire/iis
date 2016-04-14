package eu.dnetlib.iis.wf.affmatching;

import static eu.dnetlib.iis.common.string.CharSequenceUtils.toStringWithNullToEmpty;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.string.LenientComparisonStringNormalizer;
import eu.dnetlib.iis.common.string.StringNormalizer;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * 
 * Converter of {@link ExtractedDocumentMetadata} into {@link AffMatchAffiliation}
 * 
 * @author Łukasz Dumiszewski
*/

public class AffiliationConverter {

    
    private StringNormalizer organizationNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryNameNormalizer = new LenientComparisonStringNormalizer();
    
    private StringNormalizer countryCodeNormalizer = new LenientComparisonStringNormalizer();
    
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    /**
     * Converts {@link Organization} into {@link AffMatchOrganization}
     */
    public List<AffMatchAffiliation> convert(ExtractedDocumentMetadata document) {
        
        
        Preconditions.checkNotNull(document);
        Preconditions.checkArgument(StringUtils.isNotBlank(document.getId()));
        
        List<AffMatchAffiliation> affMatchAffiliations = new ArrayList<>();
        
        
        if (CollectionUtils.isEmpty(document.getAffiliations())) {
            return affMatchAffiliations;
        }
        
        
        for (int i = 0; i < document.getAffiliations().size(); i++) {
            
            Affiliation srcAffiliation = document.getAffiliations().get(i);
            
            int positionInDocument = i + 1;
            
            affMatchAffiliations.add(convertAffiliation(document.getId(), positionInDocument, srcAffiliation));
            
        }
        
        
        return affMatchAffiliations;
    }
    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private AffMatchAffiliation convertAffiliation(CharSequence documentId, int positionInDocument, Affiliation aff) {
        
        AffMatchAffiliation affMatchAff = new AffMatchAffiliation();
        
        
        affMatchAff.setDocumentId(documentId);
        
        affMatchAff.setPosition(positionInDocument);
        
        String orgName = toStringWithNullToEmpty(aff.getOrganization());
        affMatchAff.setOrganizationName(organizationNameNormalizer.normalize(orgName));
        
        String countryCode = toStringWithNullToEmpty(aff.getCountryCode());
        affMatchAff.setCountryCode(countryCodeNormalizer.normalize(countryCode));
        
        String countryName = toStringWithNullToEmpty(aff.getCountryName());
        affMatchAff.setCountryName(countryNameNormalizer.normalize(countryName));
        
        return affMatchAff;
        
    }


    //------------------------ SETTERS --------------------------

    public void setOrganizationNameNormalizer(StringNormalizer organizationNameNormalizer) {
        this.organizationNameNormalizer = organizationNameNormalizer;
    }

    public void setCountryNameNormalizer(StringNormalizer countryNameNormalizer) {
        this.countryNameNormalizer = countryNameNormalizer;
    }

    public void setCountryCodeNormalizer(StringNormalizer countryCodeNormalizer) {
        this.countryCodeNormalizer = countryCodeNormalizer;
    }
}
