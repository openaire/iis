package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.BufferedInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.common.counter.NamedCounters;
import eu.dnetlib.iis.common.counter.NamedCountersFileWriter;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeUtils;
import eu.dnetlib.openaire.exporter.model.ProjectDetail;

/**
 * {@link Project} importer reading data from stream.
 * 
 * @author mhorst
 *
 */
public class StreamingProjectImporter implements Process {
    
    private static final String PORT_OUT_PROJECT = "project";
    
    private static final String PROJECT_COUNTER_NAME = "PROJECT_COUNTER";
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    private final int progressLogInterval = 100000;
    
    private final NamedCountersFileWriter countersWriter = new NamedCountersFileWriter();
    
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
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        FileSystem fs = FileSystem.get(conf);
        
        try (DataFileWriter<Project> projectWriter = DataStore.create(
                new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_PROJECT)), Project.SCHEMA$)) {
            
            NamedCounters counters = new NamedCounters(new String[] { PROJECT_COUNTER_NAME });
            
            StreamingFacade streamingFacade = ServiceFacadeUtils.instantiate(parameters);
            try (Scanner scanner = new Scanner(new BufferedInputStream(streamingFacade.getStream()))) {

                int currentCount = 0;
                long startTime = System.currentTimeMillis();
                
                while (scanner.hasNext()) {
                    String line = scanner.nextLine();
                    if (StringUtils.isNotBlank(line)) {
                        ProjectDetail project = ProjectDetail.fromJson(line);
                        projectWriter.append(ProjectDetailConverter.convert(project));
                        counters.increment(PROJECT_COUNTER_NAME);
                        currentCount++;
                        if (currentCount%progressLogInterval==0) {
                            log.info("current progress: " + currentCount + ", last package of " + progressLogInterval + 
                                    " processed in " + ((System.currentTimeMillis()-startTime)/1000) + " secs");
                            startTime = System.currentTimeMillis();
                        }
                    }    
                }
            }
            
            countersWriter.writeCounters(counters, System.getProperty("oozie.action.output.properties"));
        }
    }


    
    
}
