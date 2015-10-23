package eu.dnetlib.iis.workflows.export.actionmanager.entity.document;

import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ENTITY_MDSTORE_ID;

import java.security.InvalidParameterException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.ProcessUtils;
import eu.dnetlib.iis.export.auxiliary.schemas.Identifier;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.workflows.export.actionmanager.entity.AbstractEntityExporterProcess;

/**
 * Document entity exporter.
 * @author mhorst
 *
 */
public class DocumentExporterProcess extends AbstractEntityExporterProcess<Identifier> {
	
	private String mdStoreId;
	
	/**
	 * Default constructor.
	 */
	public DocumentExporterProcess() {
		super(Identifier.SCHEMA$, "dmf2actions", 
				"eu/dnetlib/actionmanager/xslt/dmf2insertActions.xslt",
				StaticConfigurationProvider.NAMESPACE_PREFIX_WOS);
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
//		initializing mdStoreId for further use
		this.mdStoreId = ProcessUtils.getParameterValue(
				EXPORT_ENTITY_MDSTORE_ID, 
				conf, parameters);
		if (mdStoreId==null || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoreId)) {
			throw new InvalidParameterException("unable to export document entities to action manager, " +
					"unknown MDStore identifier. "
					+ "Required parameter '" + EXPORT_ENTITY_MDSTORE_ID + "' is missing!");
		}
		super.run(portBindings, conf, parameters);
	}

	@Override
	protected MDStoreIdWithEntityId deliverMDStoreIds(Identifier element) {
		return new MDStoreIdWithEntityId(
				this.mdStoreId, 
				element.getId().toString());
	}
}
