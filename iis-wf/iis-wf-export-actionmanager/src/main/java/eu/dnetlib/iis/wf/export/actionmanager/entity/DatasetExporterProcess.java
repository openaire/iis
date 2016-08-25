package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;

/**
 * Dataset entity exporter.
 * 
 * @author mhorst
 *
 */
public class DatasetExporterProcess extends AbstractEntityExporterProcess<DatasetToMDStore> {

    private static final String NAMESPACE_PREFIX_DATACITE = "datacite____";

    // ------------------------ CONSTRUCTORS -----------------------------

    public DatasetExporterProcess() {
        super(DatasetToMDStore.SCHEMA$, "datacite2actions",
                "eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt", NAMESPACE_PREFIX_DATACITE);
    }

    // ------------------------ LOGIC -----------------------------

    @Override
    protected MDStoreIdWithEntityId convertIdentifier(DatasetToMDStore element) {
        return new MDStoreIdWithEntityId(element.getMdStoreId().toString(), element.getDatasetId().toString());
    }

}
