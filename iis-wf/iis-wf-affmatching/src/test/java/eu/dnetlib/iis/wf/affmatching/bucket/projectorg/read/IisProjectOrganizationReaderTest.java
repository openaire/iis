package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchProjectOrganization;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.BeforeEach;
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
 * @author mhorst
 */
@ExtendWith(MockitoExtension.class)
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
    private ArgumentCaptor<Function<ProjectToOrganization, AffMatchProjectOrganization>> convertProjectOrganizationMapFunction;

    @Mock
    private JavaRDD<AffMatchProjectOrganization> projectOrganizations;

    private final String predefinedPath = "/path/to/poject_organizations/";

    @BeforeEach
    public void setUp() {
        lenient().when(avroLoader.loadJavaRDD(sparkContext, predefinedPath, ProjectToOrganization.class)).thenReturn(loadedProjectOrganizations);
        lenient().doReturn(projectOrganizations).when(loadedProjectOrganizations).map(any());
    }

    // ------------------------ TESTS --------------------------

    @Test
    public void readProjectOrganizations_NULL_CONTEXT() {
        // execute
        assertThrows(NullPointerException.class, () -> projectOrganizationReader.readProjectOrganizations(null, predefinedPath));
    }

    @Test
    public void readProjectOrganizations_NULL_PATH() {
        // execute
        assertThrows(NullPointerException.class, () -> projectOrganizationReader.readProjectOrganizations(sparkContext, null));
    }

    @Test
    public void readProjectOrganizations() throws Exception {
        // execute
        JavaRDD<AffMatchProjectOrganization> retProjectOrganization = projectOrganizationReader
                .readProjectOrganizations(sparkContext, predefinedPath);
        // assert
        assertSame(retProjectOrganization, projectOrganizations);
        verify(avroLoader).loadJavaRDD(sparkContext, predefinedPath, ProjectToOrganization.class);
        verify(loadedProjectOrganizations).map(convertProjectOrganizationMapFunction.capture());
        assertMapProjectOrganizationFunction(convertProjectOrganizationMapFunction.getValue());
    }

    // ------------------------ PRIVATE --------------------------

    private void assertMapProjectOrganizationFunction(Function<ProjectToOrganization, AffMatchProjectOrganization> function)
            throws Exception {
        // given
        ProjectToOrganization projectOrganization = mock(ProjectToOrganization.class);
        AffMatchProjectOrganization mappedProjectOrganization = mock(AffMatchProjectOrganization.class);
        when(projectOrganizationConverter.convert(projectOrganization)).thenReturn(mappedProjectOrganization);
        // execute
        AffMatchProjectOrganization retProjectOrganization = function.call(projectOrganization);
        // assert
        assertSame(retProjectOrganization, mappedProjectOrganization);
    }

}
