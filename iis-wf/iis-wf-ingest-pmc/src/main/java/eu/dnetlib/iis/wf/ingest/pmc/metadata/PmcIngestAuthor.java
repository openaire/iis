package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Helper author class used in pmc xml handlers
 * 
 * @author madryk
 *
 */
public class PmcIngestAuthor {

    private String surname;
    
    private String givenNames;
    
    private List<Integer> affiliationPos = Lists.newArrayList();
    
    private List<String> affiliationRefId = Lists.newArrayList();
    
    
    //------------------------ GETTERS --------------------------
    
    public String getSurname() {
        return surname;
    }
    
    public String getGivenNames() {
        return givenNames;
    }
    
    public List<Integer> getAffiliationPos() {
        return affiliationPos;
    }
    
    public List<String> getAffiliationRefId() {
        return affiliationRefId;
    }
    
    
    //------------------------ SETTERS --------------------------
    
    public void setSurname(String surname) {
        this.surname = surname;
    }
    
    public void setGivenNames(String givenNames) {
        this.givenNames = givenNames;
    }
    
    public void setAffiliationPos(List<Integer> affiliationPos) {
        this.affiliationPos = affiliationPos;
    }
    
    public void setAffiliationRefId(List<String> affiliationRefId) {
        this.affiliationRefId = affiliationRefId;
    }
    
    
}
