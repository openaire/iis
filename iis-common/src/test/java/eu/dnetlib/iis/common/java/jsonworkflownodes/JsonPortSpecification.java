package eu.dnetlib.iis.common.java.jsonworkflownodes;

/**
 * Specification of port containing json file.
 * 
 * @author madryk
 */
public class JsonPortSpecification {

    private String name;
    
    private String jsonFilePath;
    
    
    //------------------------ CONSTRUCTOR --------------------------
    
    public JsonPortSpecification(String name, String jsonFilePath) {
        this.name = name;
        this.jsonFilePath = jsonFilePath;
    }

    //------------------------ GETTERS --------------------------
    
    /**
     * Returns the name of the port
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the path to json file
     */
    public String getJsonFilePath() {
        return jsonFilePath;
    }
}
