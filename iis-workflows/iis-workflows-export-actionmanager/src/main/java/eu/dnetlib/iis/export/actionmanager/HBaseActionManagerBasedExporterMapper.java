package eu.dnetlib.iis.export.actionmanager;

import java.io.IOException;

import eu.dnetlib.iis.export.actionmanager.api.ActionManagerServiceFacade;
import eu.dnetlib.iis.export.actionmanager.api.HBaseActionManagerServiceFacade;


/**
 * HBase action manager based exporter.
 * @author mhorst
 *
 */
public class HBaseActionManagerBasedExporterMapper extends
		AbstractActionManagerBasedExporterMapper {
	
	@Override
	protected ActionManagerServiceFacade buildActionManager(Context context) throws IOException {
		return new HBaseActionManagerServiceFacade(context.getConfiguration());
	}
	
}
