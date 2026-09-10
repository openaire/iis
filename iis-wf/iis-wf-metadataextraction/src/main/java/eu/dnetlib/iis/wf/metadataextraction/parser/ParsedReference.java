package eu.dnetlib.iis.wf.metadataextraction.parser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Represents bibliographic reference fields parsed from a raw reference text
 * by a {@link ReferenceTextParser}.
 *
 * @author mhorst
 */
public class ParsedReference {

    private String title;
    private List<String> authors = new ArrayList<>();
    private String pages;
    private String journal;
    private String volume;
    private String year;
    private String edition;
    private String publisher;
    private String location;
    private String series;
    private String issue;
    private String url;

    /**
     * External identifiers parsed from the reference (e.g. DOI, ISBN, ISSN, arXiv),
     * keyed by the identifier type as reported by the parser. Insertion ordered.
     */
    private Map<String, String> externalIds = new LinkedHashMap<>();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors != null ? authors : new ArrayList<>();
    }

    public String getPages() {
        return pages;
    }

    public void setPages(String pages) {
        this.pages = pages;
    }

    public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getEdition() {
        return edition;
    }

    public void setEdition(String edition) {
        this.edition = edition;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getSeries() {
        return series;
    }

    public void setSeries(String series) {
        this.series = series;
    }

    public String getIssue() {
        return issue;
    }

    public void setIssue(String issue) {
        this.issue = issue;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Returns the external identifiers parsed from the reference,
     * keyed by identifier type. Never null.
     */
    public Map<String, String> getExternalIds() {
        return externalIds;
    }

    public void setExternalIds(Map<String, String> externalIds) {
        this.externalIds = externalIds != null ? externalIds : new LinkedHashMap<>();
    }

    /**
     * Registers an external identifier parsed from the reference, ignoring blank
     * types and values. The identifier type is stored as reported by the parser,
     * except for DOI which is normalized to the lowercase {@code doi} key used
     * across the metadata extraction pipeline.
     *
     * @param type identifier type (e.g. DOI, ISBN, ISSN, arXiv)
     * @param value identifier value
     */
    public void addExternalId(String type, String value) {
        if (StringUtils.isBlank(type) || StringUtils.isBlank(value)) {
            return;
        }
        String normalizedType = type.trim();
        if ("doi".equalsIgnoreCase(normalizedType)) {
            normalizedType = "doi";
        }
        externalIds.put(normalizedType, value.trim());
    }
}
