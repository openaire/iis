package eu.dnetlib.iis.wf.importer.content.approver;

import org.apache.hadoop.mapreduce.Counter;

/**
 * Wrapper of {@link ContentApprover}. It counts number of
 * invalid contents
 * 
 * @author madryk
 */
public class InvalidCountableContentApproverWrapper implements ContentApprover {

    private ContentApprover contentApprover;
    
    private Counter invalidContentCounter;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param contentApprover - internal content approver which determines if this wrapper should approve content
     * @param invalidContentCounter - counter that will be increased when wrapper will encounter invalid content
     */
    public InvalidCountableContentApproverWrapper(ContentApprover contentApprover, Counter invalidContentCounter) {
        this.contentApprover = contentApprover;
        this.invalidContentCounter = invalidContentCounter;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true if internal {@link ContentApprover} approves the given content.
     * If content is not approved, then it also increase invalid content counter.
     */
    @Override
    public boolean approve(byte[] content) {
        
        boolean result = contentApprover.approve(content);
        
        if (!result) {
            invalidContentCounter.increment(1);
        }
        
        return result;
    }
    
    
}
