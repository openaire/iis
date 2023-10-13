package eu.dnetlib.iis.wf.importer.concept.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing context record.
 * 
 * @author mhorst
 * 
 */
public class Context {

    private String id;
    private String label;
    private List<Param> params = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(final String label) {
        this.label = label;
    }

    public List<Param> getParams() {
        return params;
    }

    public void setParams(final List<Param> params) {
        this.params = params;
    }

}
