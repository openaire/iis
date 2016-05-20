package eu.dnetlib.iis.wf.importer.infospace;

import com.google.common.base.Preconditions;

/**
 * Oaf Json representation marked with qualifier.
 * @author mhorst
 *
 */
public class QualifiedOafJsonRecord {

    private final String qualifier;
   
    private final String oafJson;
    
    // ------------------------ CONSTRUCTORS --------------------------
    
    public QualifiedOafJsonRecord(String qualifier, String oafJson) {
        Preconditions.checkNotNull(qualifier);
        Preconditions.checkNotNull(oafJson);
        this.qualifier = qualifier;
        this.oafJson = oafJson;
    }
    
    // ------------------------ GETTERS --------------------------
    
    public String getQualifier() {
        return qualifier;
    }

    public String getOafJson() {
        return oafJson;
    }
}
