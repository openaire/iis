package eu.dnetlib.iis.workflows.importer.database.project;

import static eu.dnetlib.iis.workflows.importer.ImportWorkflowRuntimeParameters.IMPORT_DATABASE_SERVICE_DBNAME;
import static eu.dnetlib.iis.workflows.importer.ImportWorkflowRuntimeParameters.IMPORT_DATABASE_SERVICE_LOCATION;
import static eu.dnetlib.iis.workflows.importer.ImportWorkflowRuntimeParameters.IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT;

import java.io.StringReader;
import java.io.StringWriter;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.ws.wsaddressing.W3CEndpointReference;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import eu.dnetlib.enabling.database.rmi.DatabaseService;
import eu.dnetlib.enabling.resultset.client.ResultSetClientFactory;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.workflows.importer.dataset.DataFileRecordReceiver;

/**
 * {@link DatabaseService} based {@link Project} importer.
 * @author mhorst
 *
 */
public class DatabaseServiceBasedProjectImporter implements Process {

	private static final String PORT_OUT_PROJECT = "project";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final int defaultPagesize = 100;
	
	private final int progressLogInterval = 10000;
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_PROJECT, 
				new AvroPortType(Project.SCHEMA$));
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return outputPorts;
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		if (!parameters.containsKey(IMPORT_DATABASE_SERVICE_LOCATION)) {
			throw new InvalidParameterException("unknown database service location, "
					+ "required parameter '" + IMPORT_DATABASE_SERVICE_LOCATION + "' is missing!");
		}
		if (!parameters.containsKey(IMPORT_DATABASE_SERVICE_DBNAME)) {
			throw new InvalidParameterException("unknown database holding projects name, "
					+ "required parameter '" + IMPORT_DATABASE_SERVICE_DBNAME + "' is missing!");
		}
		
//		setting result set client read timeout
		Long rsClientReadTimeout = null;
		if (parameters.containsKey(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT)) {
			rsClientReadTimeout = Long.valueOf(
					parameters.get(IMPORT_RESULT_SET_CLIENT_READ_TIMEOUT));
		}
		
		DataFileWriter<Project> projectWriter = null;
		try {
//			initializing avro datastore writer
			projectWriter = DataStore.create(
					new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_PROJECT)), 
					Project.SCHEMA$);
//			initializing MDStore reader
			W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
			eprBuilder.address(parameters.get(IMPORT_DATABASE_SERVICE_LOCATION));
			eprBuilder.build();
			DatabaseService databaseService = new JaxwsServiceResolverImpl().getService(
					DatabaseService.class, eprBuilder.build());
			
//			reading sql query content
			StringWriter writer = new StringWriter();
			IOUtils.copy(this.getClass().getClassLoader()
	                .getResourceAsStream("eu/dnetlib/iis/workflows/importer/database/project/sql/read_project_details_v2.sql"), 
	                writer, "utf-8");
			
			W3CEndpointReference eprResult = databaseService.searchSQL(
					parameters.get(IMPORT_DATABASE_SERVICE_DBNAME), 
					writer.toString());
			log.warn("obtained ResultSet EPR: " + eprResult.toString());
			
//			obtaining resultSet
			ResultSetClientFactory rsFactory = new ResultSetClientFactory();
			if (rsClientReadTimeout!=null) {
				rsFactory.setTimeout(rsClientReadTimeout);	
			}
			rsFactory.setServiceResolver(new JaxwsServiceResolverImpl());
			rsFactory.setPageSize(defaultPagesize);
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			SAXParser saxParser = parserFactory.newSAXParser();
			int currentCount = 0;
			long startTime = System.currentTimeMillis();
			for (String record : rsFactory.getClient(eprResult)) {
				saxParser.parse(new InputSource(new StringReader(record)),
						new DatabaseProjectXmlHandler(
								new DataFileRecordReceiver<Project>(
										projectWriter)));
				currentCount++;
				if (currentCount%progressLogInterval==0) {
					log.warn("current progress: " + currentCount + 
							", last package of " + progressLogInterval + 
							" processed in " + ((System.currentTimeMillis()-startTime)/1000) + " secs");
					startTime = System.currentTimeMillis();
				}
			}
		} finally {
			if (projectWriter!=null) {
				projectWriter.close();	
			}	
		}	
	}

}
