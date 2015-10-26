package eu.dnetlib.iis.workflows.export.actionmanager.entity.dataset;

import eu.dnetlib.iis.importer.schemas.DocumentToMDStore;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.workflows.export.actionmanager.entity.AbstractEntityExporterProcess;


/**
 * Dataset entity exporter.
 * @author mhorst
 *
 */
public class DatasetExporterProcess extends AbstractEntityExporterProcess<DocumentToMDStore> {
	
	/**
	 * Default constructor.
	 */
	public DatasetExporterProcess() {
		super(DocumentToMDStore.SCHEMA$, "datacite2actions", 
				"eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt",
				StaticConfigurationProvider.NAMESPACE_PREFIX_DATACITE);
	}
	
	@Override
	protected MDStoreIdWithEntityId deliverMDStoreIds(DocumentToMDStore element) {
		return new MDStoreIdWithEntityId(
				element.getMdStoreId().toString(), 
				element.getDocumentId().toString());
	}

}
