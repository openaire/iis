package eu.dnetlib.iis.wf.importer.infospace.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.junit.Test;

import eu.dnetlib.dhp.schema.oaf.Field;
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
    private static final String JSON_EXTRA_INFO = "extra-info";


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
        eu.dnetlib.dhp.schema.oaf.Project srcProject = emptyProject(ID);

        // execute
        Project project = converter.convert(srcProject);

        // assert
        assertNull(project);
    }

    @Test
    public void convert_invalid_acronym() throws IOException {
        // given
        eu.dnetlib.dhp.schema.oaf.Project srcProject = emptyProject(ID);
        Field<String> acronym = new Field<>();
        acronym.setValue(INVALID_ACRONYM);
        srcProject.setAcronym(acronym);

        // execute
        Project project = converter.convert(srcProject);

        // assert
        assertNull(project);
    }
    
    @Test
    public void convert_no_optional() throws IOException {
        // given
        eu.dnetlib.dhp.schema.oaf.Project srcProject = emptyProject(ID);
        Field<String> code = new Field<>();
        code.setValue(GRANT_ID);
        srcProject.setCode(code);
        srcProject.setFundingtree(readFundingTreeList());

        // execute
        Project project = converter.convert(srcProject);

        // assert
        assertEquals(ID, project.getId());
        assertEquals(GRANT_ID, project.getProjectGrantId());
        assertEquals(FUNDING_CLASS, project.getFundingClass());
        assertNull(project.getProjectAcronym());
        assertEquals("{}", project.getJsonextrainfo());
        
    }

    @Test
    public void convert() throws IOException {
        // given
        eu.dnetlib.dhp.schema.oaf.Project srcProject = emptyProject(ID);
        
        Field<String> acronym = new Field<>();
        acronym.setValue(ACRONYM);
        srcProject.setAcronym(acronym);

        Field<String> code = new Field<>();
        code.setValue(GRANT_ID);
        srcProject.setCode(code);
        
        Field<String> jsonextrainfo = new Field<>();
        jsonextrainfo.setValue(JSON_EXTRA_INFO);
        srcProject.setJsonextrainfo(jsonextrainfo);
        
        srcProject.setFundingtree(readFundingTreeList());

        // execute
        Project project = converter.convert(srcProject);

        // assert
        assertEquals(ID, project.getId());
        assertEquals(ACRONYM, project.getProjectAcronym());
        assertEquals(GRANT_ID, project.getProjectGrantId());
        assertEquals(FUNDING_CLASS, project.getFundingClass());
        assertEquals(JSON_EXTRA_INFO, project.getJsonextrainfo());
        
    }

	

    // ------------------------ PRIVATE --------------------------

    private static eu.dnetlib.dhp.schema.oaf.Project emptyProject(String id) {
        eu.dnetlib.dhp.schema.oaf.Project project = new eu.dnetlib.dhp.schema.oaf.Project();
        project.setId(id);
        return project;
    }
    
    private List<Field<String>> readFundingTreeList() throws IOException {
        Field<String> result = new Field<>();
        result.setValue(readFundingTree());
        return Collections.singletonList(result);
    }

    private String readFundingTree() {
        return StaticResourceProvider
                .getResourceContent("/eu/dnetlib/iis/wf/importer/converter/fundingclass_example.xml");
    }
    
}
