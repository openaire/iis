package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.importer.schemas.DocumentToMDStore;

/**
 * Dataset entity exporter.
 * 
 * @author mhorst
 *
 */
public class DatasetExporterProcess extends AbstractEntityExporterProcess<DocumentToMDStore> {

    private static final String NAMESPACE_PREFIX_DATACITE = "datacite____";

    // ------------------------ CONSTRUCTORS -----------------------------

    public DatasetExporterProcess() {
        super(DocumentToMDStore.SCHEMA$, "datacite2actions",
                "eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt", NAMESPACE_PREFIX_DATACITE);
    }

    // ------------------------ LOGIC -----------------------------

    @Override
    protected MDStoreIdWithEntityId deliverMDStoreWithEntityId(DocumentToMDStore element) {
        return new MDStoreIdWithEntityId(element.getMdStoreId().toString(), element.getDocumentId().toString());
    }

}
