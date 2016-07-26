package eu.dnetlib.iis.wf.importer.database.project;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATABASE_SERVICE_DBNAME;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.DataFileRecordReceiver;
import eu.dnetlib.iis.wf.importer.facade.DatabaseFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;

/**
 * {@link DatabaseFacade} based {@link Project} importer.
 * @author mhorst
 *
 */
public class DatabaseServiceBasedProjectImporter implements Process {

	private static final String PORT_OUT_PROJECT = "project";
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final int progressLogInterval = 10000;
	
	private static final String queryLocation = "eu/dnetlib/iis/wf/importer/database/project/sql/read_project_details_v2.sql";
	
	private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();
	
	{
		outputPorts.put(PORT_OUT_PROJECT, new AvroPortType(Project.SCHEMA$));
	}
	
	//------------------------ LOGIC --------------------------
	
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
	    
		Preconditions.checkArgument(parameters.containsKey(IMPORT_DATABASE_SERVICE_DBNAME), 
                "unknown database holding projects name, required parameter '%s' is missing!", IMPORT_DATABASE_SERVICE_DBNAME);
		
		FileSystem fs = FileSystem.get(conf);
		
		try (DataFileWriter<Project> projectWriter = DataStore.create(
		        new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_PROJECT)), Project.SCHEMA$)) {

//			initializing database reader
			DatabaseFacade databaseFacade = ServiceFacadeUtils.instantiate(parameters);

			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			SAXParser saxParser = parserFactory.newSAXParser();
			int currentCount = 0;
			long startTime = System.currentTimeMillis();
			
			for (String record : databaseFacade.searchSQL(parameters.get(IMPORT_DATABASE_SERVICE_DBNAME), loadQuery())) {
				saxParser.parse(new InputSource(new StringReader(record)), new DatabaseProjectXmlHandler(new DataFileRecordReceiver<Project>(projectWriter)));
				currentCount++;
				if (currentCount%progressLogInterval==0) {
					log.debug("current progress: " + currentCount + ", last package of " + progressLogInterval + 
							" processed in " + ((System.currentTimeMillis()-startTime)/1000) + " secs");
					startTime = System.currentTimeMillis();
				}
			}
		}
	}
	
    //------------------------ LOGIC --------------------------

	/**
	 * Reads query from classpath resource.
	 */
	private String loadQuery() throws IOException {
	    try (StringWriter writer = new StringWriter()) {
	        IOUtils.copy(this.getClass().getClassLoader().getResourceAsStream(queryLocation), writer, "utf-8");
	        return writer.toString();    
	    }
	}

}
