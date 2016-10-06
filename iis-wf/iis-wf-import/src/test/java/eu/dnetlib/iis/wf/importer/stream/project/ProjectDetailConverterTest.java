package eu.dnetlib.iis.wf.importer.stream.project;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import eu.dnetlib.iis.common.TestsIOUtils;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.openaire.exporter.model.ProjectDetail;

/**
 * @author mhorst
 *
 */
public class ProjectDetailConverterTest {

    private static String outputResourceLocation = "/eu/dnetlib/iis/wf/importer/stream/project/data/output/project.json";
    
    @Test
    public void testConversion() throws Exception {
        
        ProjectDetailConverter converter = new ProjectDetailConverter();
        
        List<Project> expected = JsonUtils.convertToList(
                StreamingFacadeMockFactory.class.getResourceAsStream(outputResourceLocation), 
                Project.SCHEMA$, Project.class);
        
        StreamingFacade streamingFacade = new StreamingFacadeMockFactory().instantiate(null);
        
        List<Project> actual = new ArrayList<>();
        
        try (Scanner scanner = new Scanner(new BufferedInputStream(streamingFacade.getStream()))) {
            
                String line = getNonEmptyLine(scanner);
                assertNotNull(line);
                ProjectDetail projectDetail = ProjectDetail.fromJson(line);
                assertNotNull(projectDetail);
                actual.add(converter.convert(projectDetail));
                
                line = getNonEmptyLine(scanner);
                assertNotNull(line);
                projectDetail = ProjectDetail.fromJson(line);
                assertNotNull(projectDetail);
                actual.add(converter.convert(projectDetail));
                
                line = getNonEmptyLine(scanner);
                assertNull(line);
        }
        
        TestsIOUtils.assertEqualSets(expected, actual, true);
        
    }
    
    private String getNonEmptyLine(Scanner scanner) {
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            if (StringUtils.isNotBlank(line)) {
                return line;
            }
        }
        return null;
    }
    
}
