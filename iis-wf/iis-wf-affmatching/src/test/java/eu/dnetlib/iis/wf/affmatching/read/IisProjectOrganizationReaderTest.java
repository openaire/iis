package eu.dnetlib.iis.wf.affmatching.read;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.model.ProjectOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author mhorst
 */
@RunWith(MockitoJUnitRunner.class)
public class IisProjectOrganizationReaderTest {

    @InjectMocks
    private IisProjectOrganizationReader projectOrganizationReader = new IisProjectOrganizationReader();
    
    @Mock
    private SparkAvroLoader avroLoader;
    
    @Mock
    private ProjectOrganizationConverter projectOrganizationConverter;
    
    @Mock
    private JavaSparkContext sparkContext;
    
    @Mock
    private JavaRDD<ProjectToOrganization> loadedProjectOrganizations;
    
    @Captor
    private ArgumentCaptor<Function<ProjectToOrganization, ProjectOrganization>> mapProjectOrganizationFunction;
    
    @Mock
    private JavaRDD<ProjectOrganization> projectOrganizations;
    
    private final String predefinedPath = "/path/to/poject_organizations/";
    
    @Before
    public void setUp() {
        doReturn(loadedProjectOrganizations).when(avroLoader).loadJavaRDD(sparkContext, predefinedPath, ProjectToOrganization.class);
        doReturn(projectOrganizations).when(loadedProjectOrganizations).map(any());
    }
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void readProjectOrganization_NULL_CONTEXT() {
        projectOrganizationReader.readProjectOrganization(null, predefinedPath);
    }
    
    @Test(expected = NullPointerException.class)
    public void readProjectOrganization_NULL_PATH() {
        projectOrganizationReader.readProjectOrganization(sparkContext, null);
    }
    
    @Test
    public void readProjectOrganization() throws Exception {
        JavaRDD<ProjectOrganization> retProjectOrganization = projectOrganizationReader.readProjectOrganization(
        		sparkContext, predefinedPath);
        
        assertTrue(retProjectOrganization == projectOrganizations);
        verify(avroLoader).loadJavaRDD(sparkContext, predefinedPath, ProjectToOrganization.class);
        verify(loadedProjectOrganizations).map(mapProjectOrganizationFunction.capture());
        assertMapProjectOrganizationFunction(mapProjectOrganizationFunction.getValue());
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void assertMapProjectOrganizationFunction(Function<ProjectToOrganization, ProjectOrganization> function) throws Exception {
        ProjectToOrganization projectOrganization = mock(ProjectToOrganization.class);
        ProjectOrganization mappedProjectOrganization = mock(ProjectOrganization.class);
        when(projectOrganizationConverter.convert(projectOrganization)).thenReturn(mappedProjectOrganization);
        ProjectOrganization retProjectOrganization = function.call(projectOrganization);
        assertTrue(retProjectOrganization == mappedProjectOrganization);
    }
    
}
