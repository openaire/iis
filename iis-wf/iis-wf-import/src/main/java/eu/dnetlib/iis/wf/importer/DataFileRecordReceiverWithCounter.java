package eu.dnetlib.iis.wf.importer;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

/**
 * {@link DataFileWriter} based record receiver with counter of
 * received records.
 * 
 * @author madryk
 */
public class DataFileRecordReceiverWithCounter<T> extends DataFileRecordReceiver<T> {

    private long receivedCount = 0L;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param writer - writer of the received records
     */
    public DataFileRecordReceiverWithCounter(DataFileWriter<T> writer) {
        super(writer);
    }
    
    
    //------------------------ GETTERS --------------------------
    
    /**
     * Returns number of received records
     */
    public long getReceivedCount() {
        return receivedCount;
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Receives passed record and increments the counter.
     */
    @Override
    public void receive(T record) throws IOException {
        super.receive(record);
        ++receivedCount;
    }
}
