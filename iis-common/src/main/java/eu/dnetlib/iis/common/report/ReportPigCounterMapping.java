package eu.dnetlib.iis.common.report;

/**
 * Mapping between source pig counter and destination report counter.
 * 
 * @author madryk
 */
public class ReportPigCounterMapping {

    private String destReportCounterName;
    
    private String sourcePigJobAlias;
    
    private String sourcePigJobCounterName;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     */
    public ReportPigCounterMapping(String destReportCounterName, String sourcePigJobAlias, String sourcePigJobCounterName) {
        this.destReportCounterName = destReportCounterName;
        this.sourcePigJobAlias = sourcePigJobAlias;
        this.sourcePigJobCounterName = sourcePigJobCounterName;
    }


    //------------------------ GETTERS --------------------------

    /**
     * Returns name of the destination report counter corresponding to the source pig counter.
     */
    public String getDestReportCounterName() {
        return destReportCounterName;
    }


    /**
     * Returns job alias of the source pig counter corresponding to the destination report counter.
     */
    public String getSourcePigJobAlias() {
        return sourcePigJobAlias;
    }


    /**
     * Returns name of source pig counter corresponding to the destination report counter.
     */
    public String getSourcePigJobCounterName() {
        return sourcePigJobCounterName;
    }
}
