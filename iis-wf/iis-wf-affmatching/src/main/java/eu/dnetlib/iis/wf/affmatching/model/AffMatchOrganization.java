package eu.dnetlib.iis.wf.affmatching.model;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Organization data that the affiliation matching module operates on.
 * @author Łukasz Dumiszewski
 *
 */
public class AffMatchOrganization {
    
    
    private String id;
    
    private String name;
    
    private String shortName;
    
    private String countryName;
    
    private String countryCode;
    
    private String websiteUrl;

    private List<String> alternativeNames = Lists.newArrayList();
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public AffMatchOrganization(String id) {
        Preconditions.checkArgument(StringUtils.isNotBlank(id));
        this.id = id;
    }
    
    //------------------------ GETTERS --------------------------
    
    /**
     * Id of the organization
     */
    public String getId() {
        return id;
    }

    /**
     * Name of the organization, e.g. Interdisciplinary Centre of Modeling
     */
    public String getName() {
        return name;
    }

    /**
     * Short name of the organization, e.g. ICM
     */
    public String getShortName() {
        return shortName;
    }

    /**
     * Name of the organization's country
     */
    public String getCountryName() {
        return countryName;
    }

    /**
     * ISO code of the organization's country
     */
    public String getCountryCode() {
        return countryCode;
    }

    /**
     * Organization's website url 
     */
    public String getWebsiteUrl() {
        return websiteUrl;
    }

    /**
     * Alternative names of the organization, e.g. name
     * in different languages than {@link #getName()}
     */
    public List<String> getAlternativeNames() {
        return alternativeNames;
    }


    //------------------------ LOGIC --------------------------
    
    /**
     * Adds the name to the alternative names list
     */
    public void addAlternativeName(String alternativeName) {
        this.alternativeNames.add(alternativeName);
    }
    
    /**
     * Returns the list with all organization names.
     * That is main {@link #getName()} with all {@link #getAlternativeNames()} 
     */
    public List<String> getNames() {
        List<String> names = Lists.newArrayList(name);
        names.addAll(alternativeNames);
        
        return names;
    }
    
    
    //------------------------ SETTERS --------------------------
    
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }


    
    //------------------------ HashCode & Equals --------------------------

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
         }
         
         if (getClass() != obj.getClass()) {
            return false;
         }
         
         final AffMatchOrganization other = (AffMatchOrganization) obj;
         
         return Objects.equals(id, other.id);
    }


    //------------------------ toString --------------------------

    @Override
    public String toString() {
        return "AffMatchOrganization [id=" + id + ", name=" + name + ", shortName=" + shortName
                + ", countryName=" + countryName + ", countryCode=" + countryCode + ", websiteUrl="
                + websiteUrl + "]";
    }
    
    


}
