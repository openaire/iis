package eu.dnetlib.iis.wf.affmatching.read;

import static eu.dnetlib.iis.common.string.CharSequenceUtils.toStringWithNullToEmpty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;
import eu.dnetlib.iis.metadataextraction.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * 
 * Converter of {@link ExtractedDocumentMetadata} into {@link AffMatchAffiliation}
 * 
 * @author Łukasz Dumiszewski
*/

public class AffiliationConverter implements Serializable {


    private static final long serialVersionUID = 1L;

    private BiFunction<Integer, ExtractedDocumentMetadata, Boolean> documentAcceptor =
            (BiFunction<Integer, ExtractedDocumentMetadata, Boolean> & Serializable)
                    (position, document) -> isAuthorAffiliation(position, document.getAuthors());
    
    
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
            
            if (documentAcceptor.apply(i, document)) {
                affMatchAffiliations.add(convertAffiliation(document.getId(), i + 1, srcAffiliation));    
            }
            
        }
        
        
        return affMatchAffiliations;
    }
    
    
    
    //------------------------ PRIVATE --------------------------
    
    private static boolean isAuthorAffiliation(int position, List<Author> authors) {
        if (CollectionUtils.isNotEmpty(authors)) {
          for (Author author : authors) {
              if (CollectionUtils.isNotEmpty(author.getAffiliationPositions())) {
                  for (Integer currentPosition : author.getAffiliationPositions()) {
                      if (position == currentPosition) {
                          return true;
                      }
                  }
              }
          }
        }
        return false;
    }
    
    private AffMatchAffiliation convertAffiliation(CharSequence documentId, int positionInDocument, Affiliation aff) {
        
        AffMatchAffiliation affMatchAff = new AffMatchAffiliation(documentId.toString(), positionInDocument);
        
        
        String orgName = toStringWithNullToEmpty(aff.getOrganization());
        affMatchAff.setOrganizationName(orgName);
        
        String countryCode = toStringWithNullToEmpty(aff.getCountryCode());
        affMatchAff.setCountryCode(countryCode);
        
        String countryName = toStringWithNullToEmpty(aff.getCountryName());
        affMatchAff.setCountryName(countryName);
        
        return affMatchAff;
        
    }

    public void setDocumentAcceptor(BiFunction<Integer, ExtractedDocumentMetadata, Boolean> documentAcceptor) {
        this.documentAcceptor = documentAcceptor;
    }
}
