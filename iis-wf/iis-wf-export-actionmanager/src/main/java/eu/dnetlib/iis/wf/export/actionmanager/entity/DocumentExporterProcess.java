package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ENTITY_MDSTORE_ID;

import java.security.InvalidParameterException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.schemas.Identifier;

/**
 * Document entity exporter.
 * 
 * @author mhorst
 *
 */
public class DocumentExporterProcess extends AbstractEntityExporterProcess<Identifier> {

    private String mdStoreId;

    // ------------------------ CONSTRUCTORS -----------------------------

    public DocumentExporterProcess() {
        super(Identifier.SCHEMA$, "dmf2actions", "eu/dnetlib/actionmanager/xslt/dmf2insertActions.xslt");
    }

    // ------------------------ LOGIC -----------------------------

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        this.mdStoreId = ProcessUtils.getParameterValue(EXPORT_ENTITY_MDSTORE_ID, conf, parameters);
        if (StringUtils.isBlank(mdStoreId) || WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoreId)) {
            throw new InvalidParameterException("unable to export document entities to action manager, "
                    + "unknown MDStore identifier. Required parameter '" + EXPORT_ENTITY_MDSTORE_ID + "' is missing!");
        }
        super.run(portBindings, conf, parameters);
    }

    @Override
    protected MDStoreIdWithEntityId convertIdentifier(Identifier element) {
        return new MDStoreIdWithEntityId(this.mdStoreId, element.getId().toString());
    }
}
