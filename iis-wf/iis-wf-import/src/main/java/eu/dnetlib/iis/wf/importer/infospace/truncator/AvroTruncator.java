package eu.dnetlib.iis.wf.importer.infospace.truncator;

import org.apache.avro.specific.SpecificRecord;

import java.io.Serializable;

public interface AvroTruncator<T extends SpecificRecord> extends Serializable {

    /**
     * Truncates source instance. For performance reasons truncation should reuse source object.
     *
     * @param source Instance to be truncated.
     * @return Truncated instance.
     */
    T truncate(T source);
}
