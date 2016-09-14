package eu.dnetlib.iis.wf.importer.database.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.RecordReceiver;

public class DatabaseProjectXmlHandlerTest {

    private String projectId = "40|arc_________::884b76698f227ecf30a1ace12071189a";
    private String projectGrantId = "ANZCCART";
    private String projectAcronym = "ANZ";
    private String fundingClass = "ARC::ARC Centres of Excellences";
    private String jsonExtraInfo = "jsonextrainfo";
    
    private String rootTestResourcesPath = "/eu/dnetlib/iis/wf/importer/database/project/data/input/";
    
    private InputStream inputStream;
    
    private SAXParser saxParser;
    
    private ProjectsReceiver projectsReceiver;
    
    private DatabaseProjectXmlHandler handler;
    
    
    //------------------------ TESTS --------------------------
    
    @Before
    public void initialize() throws ParserConfigurationException, SAXException {
        saxParser = SAXParserFactory.newInstance().newSAXParser();
        projectsReceiver = new ProjectsReceiver();
        handler = new DatabaseProjectXmlHandler(projectsReceiver);
    }
    
    @After
    public void finalize() throws IOException {
        if (inputStream!=null) {
            inputStream.close();
        }
    }
    
    @Test
    public void testProjectImportWithOptional() throws Exception {
        // given
        String filePath = rootTestResourcesPath + "project_with_optional.xml";
        
        // execute        
        saxParser.parse(inputStream = DatabaseProjectXmlHandlerTest.class.getResourceAsStream(filePath), handler);
        
        // assert
        assertEquals(1, projectsReceiver.getProjects().size());
        Project project = projectsReceiver.getProjects().get(0);
        assertEquals(projectId, project.getId());
        assertEquals(projectGrantId, project.getProjectGrantId());
        assertEquals(projectAcronym, project.getProjectAcronym());
        assertEquals(fundingClass, project.getFundingClass());
        assertEquals(jsonExtraInfo, project.getJsonextrainfo());
    }
    
    @Test
    public void testProjectImportNoOptionalNoFundingTree() throws Exception {
        // given
        String filePath = rootTestResourcesPath + "project_no_optional_no_fundingtree.xml";
        
        // execute        
        saxParser.parse(inputStream = DatabaseProjectXmlHandlerTest.class.getResourceAsStream(filePath), handler);
        
        // assert
        assertEquals(1, projectsReceiver.getProjects().size());
        Project project = projectsReceiver.getProjects().get(0);
        assertEquals(projectId, project.getId());
        assertEquals(projectGrantId, project.getProjectGrantId());
        assertNull(project.getProjectAcronym());
        assertNull(project.getFundingClass());
        assertNull(project.getJsonextrainfo());
    }
    
    @Test
    public void testProjectImportWithNullAcronymNullOptional() throws Exception {
        // given
        String filePath = rootTestResourcesPath + "project_null_acronym_null_optional.xml";
        
        // execute        
        saxParser.parse(inputStream = DatabaseProjectXmlHandlerTest.class.getResourceAsStream(filePath), handler);
        
        // assert
        assertEquals(1, projectsReceiver.getProjects().size());
        Project project = projectsReceiver.getProjects().get(0);
        assertEquals(projectId, project.getId());
        assertEquals(projectGrantId, project.getProjectGrantId());
        assertNull(project.getProjectAcronym());
        assertEquals(fundingClass, project.getFundingClass());
        assertNull(project.getJsonextrainfo());
    }
    
    //------------------------ INNER CLASS --------------------------
    
    private static class ProjectsReceiver implements RecordReceiver<Project> {
        
        private final List<Project> projects = new ArrayList<Project>();

        //------------------------ GETTERS --------------------------
        
        public List<Project> getProjects() {
            return projects;
        }
        
        //------------------------ LOGIC --------------------------
        @Override
        public void receive(Project object) throws IOException {
            projects.add(object);
            
        }
    }

    
}
