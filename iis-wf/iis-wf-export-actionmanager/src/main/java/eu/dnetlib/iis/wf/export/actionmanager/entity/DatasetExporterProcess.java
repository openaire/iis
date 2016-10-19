package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;

/**
 * Dataset entity exporter.
 * 
 * @author mhorst
 *
 */
public class DatasetExporterProcess extends AbstractEntityExporterProcess<DatasetToMDStore> {

    // ------------------------ CONSTRUCTORS -----------------------------

    public DatasetExporterProcess() {
        super(DatasetToMDStore.SCHEMA$, "datacite2actions",
                "eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt");
    }

    // ------------------------ LOGIC -----------------------------

    @Override
    protected MDStoreIdWithEntityId convertIdentifier(DatasetToMDStore element) {
        return new MDStoreIdWithEntityId(element.getMdStoreId().toString(), element.getDatasetId().toString());
    }

}
