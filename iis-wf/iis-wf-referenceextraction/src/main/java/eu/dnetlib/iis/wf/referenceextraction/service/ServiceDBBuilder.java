package eu.dnetlib.iis.wf.referenceextraction.service;

import eu.dnetlib.iis.importer.schemas.Service;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process creating a database reading {@link Service} input avro records.
 *
 * @author mhorst
 */
public class ServiceDBBuilder extends AbstractDBBuilder<Service> {
    

    // -------------------------- CONSTRUCTORS ------------------------------

    public ServiceDBBuilder() {
        super(Service.SCHEMA$);
    }

}
