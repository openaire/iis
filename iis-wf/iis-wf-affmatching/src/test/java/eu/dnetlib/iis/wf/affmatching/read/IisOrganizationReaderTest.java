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

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class IisOrganizationReaderTest {

    @InjectMocks
    private IisOrganizationReader organizationReader = new IisOrganizationReader();
    
    @Mock
    private SparkAvroLoader avroLoader;
    
    @Mock
    private OrganizationConverter organizationConverter;
    
    
    @Mock
    private JavaSparkContext sparkContext;
    
    
    @Mock
    private JavaRDD<Organization> loadedOrganizations;
    
    @Captor
    private ArgumentCaptor<Function<Organization, AffMatchOrganization>> mapOrganizationFunction;
    
    @Mock
    private JavaRDD<AffMatchOrganization> organizations;
    
    
    @Before
    public void setUp() {
        
        doReturn(loadedOrganizations).when(avroLoader).loadJavaRDD(sparkContext, "/path/to/organizations/", Organization.class);
        doReturn(organizations).when(loadedOrganizations).map(any());
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test(expected = NullPointerException.class)
    public void readOrganizations_NULL_CONTEXT() {
        
        // execute
        organizationReader.readOrganizations(null, "/path/to/organizations/");
        
    }
    
    
    @Test(expected = NullPointerException.class)
    public void readOrganizations_NULL_PATH() {
        
        // execute
        organizationReader.readOrganizations(sparkContext, null);
        
    }
    
    
    @Test
    public void readOrganizations() throws Exception {
        
        // execute
        
        JavaRDD<AffMatchOrganization> retOrganizations = organizationReader.readOrganizations(sparkContext, "/path/to/organizations/");
        
        
        // assert
        
        assertTrue(retOrganizations == organizations);
        
        verify(avroLoader).loadJavaRDD(sparkContext, "/path/to/organizations/", Organization.class);
        
        verify(loadedOrganizations).map(mapOrganizationFunction.capture());
        assertMapOrganizationFunction(mapOrganizationFunction.getValue());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertMapOrganizationFunction(Function<Organization, AffMatchOrganization> function) throws Exception {
        
        Organization organization = mock(Organization.class);
        AffMatchOrganization mappedOrganization = mock(AffMatchOrganization.class);
        
        when(organizationConverter.convert(organization)).thenReturn(mappedOrganization);
        
        
        AffMatchOrganization retOrganization = function.call(organization);
        
        
        assertTrue(retOrganization == mappedOrganization);
    }
    
}
