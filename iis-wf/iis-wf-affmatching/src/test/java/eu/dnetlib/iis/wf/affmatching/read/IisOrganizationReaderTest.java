package eu.dnetlib.iis.wf.affmatching.read;

import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/

@ExtendWith(MockitoExtension.class)
public class IisOrganizationReaderTest {

    
    
    @InjectMocks
    private IisOrganizationReader reader = new IisOrganizationReader();
    
    @Mock
    private OrganizationConverter organizationConverter;
    
    @Mock
    private SparkAvroLoader sparkAvroLoader;
    
    @Mock
    private JavaSparkContext sparkContext;

    @Mock
    private JavaRDD<Organization> inputOrganizations;

    @Mock
    private JavaRDD<AffMatchOrganization> affMatchOrganizations;

    
    @Captor
    private ArgumentCaptor<Function<Organization, AffMatchOrganization>> convertFunction;

    
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void readOrganizations_sparkContext_null() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                reader.readOrganizations(null, "/aaa"));
        
    }
    

    @Test
    public void readOrganizations_inputPath_blank() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> reader.readOrganizations(sparkContext, "  "));
        
    }
    
    @Test
    public void readOrganizations() throws Exception {
        
        // given
        
        String inputPath = "/data/organizations";
        
        
        when(sparkAvroLoader.loadJavaRDD(sparkContext, inputPath, Organization.class)).thenReturn(inputOrganizations);
        
        doReturn(affMatchOrganizations).when(inputOrganizations).map(any());

        
        // execute
        
        JavaRDD<AffMatchOrganization> retAffMatchOrganizations = reader.readOrganizations(sparkContext, inputPath);
        
        
        // assert

        assertSame(affMatchOrganizations, retAffMatchOrganizations);
        
        verify(inputOrganizations).map(convertFunction.capture());
        assertConvertFunction(convertFunction.getValue());
    }
    
    
    
    //------------------------ TESTS --------------------------

    
    private void assertConvertFunction(Function<Organization, AffMatchOrganization> function) throws Exception {

        // given
        
        Organization org = new Organization();
        org.setId("ORG1");
        
        AffMatchOrganization affMatchOrg = new AffMatchOrganization("ORG1");
        
        when(organizationConverter.convert(org)).thenReturn(affMatchOrg);

        
        // execute
        
        AffMatchOrganization retAffMatchOrg = function.call(org);

        
        // assert

        assertSame(retAffMatchOrg, affMatchOrg);
        
    }
}
