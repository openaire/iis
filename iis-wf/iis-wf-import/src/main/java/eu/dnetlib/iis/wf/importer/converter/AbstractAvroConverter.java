package eu.dnetlib.iis.wf.importer.converter;

import org.apache.hadoop.hbase.client.Result;

import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * Abstract Hbase {@link Result} to avro object converter.
 * 
 * @author mhorst
 *
 * @param <T>
 */
public abstract class AbstractAvroConverter<T> implements AvroConverter<T> {

    /**
     * Result approver.
     */
    protected final ResultApprover resultApprover;

    /**
     * Default constructor.
     * 
     * @param resultApprover
     */
    public AbstractAvroConverter(ResultApprover resultApprover) {
        this.resultApprover = resultApprover;
    }

}
