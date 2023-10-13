package eu.dnetlib.iis.wf.importer.concept.model;

/**
 * Class representing context parameter.
 * 
 * @author mhorst
 */
public class Param {

    private String name;

    private String value;

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public Param setName(final String name) {
        this.name = name;
        return this;
    }

    public Param setValue(final String value) {
        this.value = value;
        return this;
    }

}
