package eu.dnetlib.iis.wf.referenceextraction.dataset;

import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process building datasets database reading {@link DataSetReference} input avro records.
 *
 * @author mhorst
 */
public class DatasetDBBuilder extends AbstractDBBuilder<DataSetReference> {

    // -------------------------- CONSTRUCTORS ------------------------------

    public DatasetDBBuilder() {
        super(DataSetReference.SCHEMA$);
    }

}
