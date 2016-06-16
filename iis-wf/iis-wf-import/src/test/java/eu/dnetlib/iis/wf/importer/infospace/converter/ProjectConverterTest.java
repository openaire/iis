package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafEntity.Builder;
import eu.dnetlib.data.proto.ProjectProtos.Project.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 * {@link ProjectConverter} test class.
 * @author mhorst
 *
 */
public class ProjectConverterTest {

    private static final String ID = "an identifier";
    private static final String ACRONYM = "TLA";
    private static final String INVALID_ACRONYM = "UNDEFINED";
    private static final String GRANT_ID = "another identifier";
    private static final String FUNDING_CLASS = "WT::WT";


    private ProjectConverter converter = new ProjectConverter();

    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void convert_null_oafEntity() throws IOException {
        // execute
        converter.convert(null);
    }

    @Test
    public void convert_unset_project() throws IOException {
        // given
        OafEntity oafEntity = emptyEntityBuilder(ID).build();

        // execute
        Project project = converter.convert(oafEntity);

        // assert
        assertNull(project);
    }

    @Test
    public void convert_invalid_acronym() throws IOException {
        // given
        OafEntity.Builder builder = emptyEntityBuilder(ID);

        Metadata.Builder mdBuilder = builder.getProjectBuilder().getMetadataBuilder();
        mdBuilder.getAcronymBuilder().setValue(INVALID_ACRONYM);

        OafEntity oafEntity = builder.build();

        // execute
        Project project = converter.convert(oafEntity);

        // assert
        assertNull(project);
    }

    @Test
    public void convert() throws IOException {
        // given
        OafEntity.Builder builder = emptyEntityBuilder(ID);

        Metadata.Builder mdBuilder = builder.getProjectBuilder().getMetadataBuilder();
        mdBuilder.getAcronymBuilder().setValue(ACRONYM);
        mdBuilder.getCodeBuilder().setValue(GRANT_ID);
        mdBuilder.addFundingtreeBuilder().setValue(readFundingTree());

        OafEntity oafEntity = builder.build();

        // execute
        Project project = converter.convert(oafEntity);

        // assert
        assertEquals(ID, project.getId());
        assertEquals(ACRONYM, project.getProjectAcronym());
        assertEquals(GRANT_ID, project.getProjectGrantId());
        assertEquals(FUNDING_CLASS, project.getFundingClass());
    }

	@Test
	public void testFundingClassExtraction() throws Exception {
		List<String> fundingTreeList = Collections.singletonList(readFundingTree());
		String fundingClass = ProjectConverter.extractFundingClass(fundingTreeList);
		assertNotNull(fundingClass);
		assertEquals(FUNDING_CLASS, fundingClass);
		
	}

    // ------------------------ PRIVATE --------------------------

    private static Builder emptyEntityBuilder(String id) {
        // note that the type does not matter for the converter
        return OafEntity.newBuilder().setType(Type.project).setId(id);
    }

    private String readFundingTree() throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/iis/wf/importer/converter/fundingclass_example.xml"), "utf8");
    }
}
