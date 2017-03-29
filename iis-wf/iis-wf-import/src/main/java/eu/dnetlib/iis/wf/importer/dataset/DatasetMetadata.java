package eu.dnetlib.iis.wf.importer.dataset;

import java.util.List;
import java.util.Map;

/**
 * Record grouping raw dataset metadata.
 * 
 * @author mhorst
 *
 */
public class DatasetMetadata {

    private String headerId;
    private String idType;
    private String idValue;
    private List<CharSequence> creatorNames;
    private List<CharSequence> titles;
    private List<CharSequence> formats;
    private Map<CharSequence, CharSequence> alternateIdentifiers;
    private String description;
    private String publisher;
    private String publicationYear;
    private String resourceTypeClass;
    private String resourceTypeValue;
    private String currentAlternateIdentifierType;

    public String getHeaderId() {
        return headerId;
    }
    public void setHeaderId(String headerId) {
        this.headerId = headerId;
    }
    public String getIdType() {
        return idType;
    }
    public void setIdType(String idType) {
        this.idType = idType;
    }
    public String getIdValue() {
        return idValue;
    }
    public void setIdValue(String idValue) {
        this.idValue = idValue;
    }
    public List<CharSequence> getCreatorNames() {
        return creatorNames;
    }
    public void setCreatorNames(List<CharSequence> creatorNames) {
        this.creatorNames = creatorNames;
    }
    public List<CharSequence> getTitles() {
        return titles;
    }
    public void setTitles(List<CharSequence> titles) {
        this.titles = titles;
    }
    public List<CharSequence> getFormats() {
        return formats;
    }
    public void setFormats(List<CharSequence> formats) {
        this.formats = formats;
    }
    public Map<CharSequence, CharSequence> getAlternateIdentifiers() {
        return alternateIdentifiers;
    }
    public void setAlternateIdentifiers(Map<CharSequence, CharSequence> alternateIdentifiers) {
        this.alternateIdentifiers = alternateIdentifiers;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public String getPublisher() {
        return publisher;
    }
    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }
    public String getPublicationYear() {
        return publicationYear;
    }
    public void setPublicationYear(String publicationYear) {
        this.publicationYear = publicationYear;
    }
    public String getResourceTypeClass() {
        return resourceTypeClass;
    }
    public void setResourceTypeClass(String resourceTypeClass) {
        this.resourceTypeClass = resourceTypeClass;
    }
    public String getResourceTypeValue() {
        return resourceTypeValue;
    }
    public void setResourceTypeValue(String resourceTypeValue) {
        this.resourceTypeValue = resourceTypeValue;
    }
    public String getCurrentAlternateIdentifierType() {
        return currentAlternateIdentifierType;
    }
    public void setCurrentAlternateIdentifierType(String currentAlternateIdentifierType) {
        this.currentAlternateIdentifierType = currentAlternateIdentifierType;
    }
    
}
