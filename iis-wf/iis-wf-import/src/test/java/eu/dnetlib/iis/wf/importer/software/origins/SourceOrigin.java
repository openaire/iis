package eu.dnetlib.iis.wf.importer.software.origins;

import java.io.Serializable;

/**
 * Class representing Origin entity stored in ORC files in the SoftwareHeritage graph dump.
 * @author mhorst
 */
public class SourceOrigin implements Serializable {
    
    private static final long serialVersionUID = -1809996783546970947L;
    
    private String id;
    
    private String url;

    // Constructors
    public SourceOrigin(String id, String url) {
        this.id = id;
        this.url = url;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}