package eu.dnetlib.iis.common.report;

import com.google.common.base.Preconditions;

/**
 * Representation of mapping between a source pig counter and a destination report counter.
 * 
 * @author madryk
 */
public class ReportPigCounterMapping {

    private String sourcePigCounterName;
    
    private String sourcePigJobAlias;
    
    private String destReportCounterName;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * 
     * @param sourcePigCounterName name of the counter in pig 
     * @param sourcePigJobAlias alias of the pig job the pig counter belongs to, null for root level counters
     * @param destReportCounterName target name of the counter (as will be seen in the report), may not be null
     * 
     * @throws NullPinterException if sourcePigCounterName or destReportCounterName is null
     */
    public ReportPigCounterMapping(String sourcePigCounterName, String sourcePigJobAlias, String destReportCounterName) {
        
        Preconditions.checkNotNull(destReportCounterName);
        Preconditions.checkNotNull(sourcePigCounterName);
        
        this.destReportCounterName = destReportCounterName;
        this.sourcePigJobAlias = sourcePigJobAlias;
        this.sourcePigCounterName = sourcePigCounterName;
    }

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if this mapping relates to the root level pig counter. Returns false if this is the job counter mapping.
     */
    public boolean isRootLevelCounterMapping() {
        return sourcePigJobAlias == null;
    }

    //------------------------ GETTERS --------------------------

    /**
     * Returns the name of the destination report counter corresponding to the source pig counter.
     */
    public String getDestReportCounterName() {
        return destReportCounterName;
    }


    /**
     * Returns the job alias of the source pig counter corresponding to the destination report counter.
     * Null for root level counters
     */
    public String getSourcePigJobAlias() {
        return sourcePigJobAlias;
    }


    /**
     * Returns the name of the source pig counter corresponding to the destination report counter.
     */
    public String getSourcePigCounterName() {
        return sourcePigCounterName;
    }
}
