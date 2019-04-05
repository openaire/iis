package eu.dnetlib.iis.wf.importer.software.origins;

/**
 * Origin entry retrieved from SoftwareHeritage endpoint.
 * @author mhorst
 *
 */
public class SoftwareHeritageOriginEntry {

    private String id;

    private String originVisitsUrl;
    
    private String type;
    
    private String url;

    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOriginVisitsUrl() {
        return originVisitsUrl;
    }

    public void setOriginVisitsUrl(String originVisitsUrl) {
        this.originVisitsUrl = originVisitsUrl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
}
