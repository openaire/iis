package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 * {@link eu.dnetlib.dhp.schema.oaf.Project} containing project details to {@link Project} converter.
 * 
 * @author mhorst
 *
 */
public class ProjectConverter implements OafEntityToAvroConverter<eu.dnetlib.dhp.schema.oaf.Project, Project> {

    protected static final Logger log = Logger.getLogger(ProjectConverter.class);

    public static final String BLANK_JSONEXTRAINFO = "{}";

    private static final Set<String> ACRONYM_SKIP_LOWERCASED_VALUES = new HashSet<String>(
            Arrays.asList("undefined", "unknown"));
    
    private final FundingTreeParser fundingTreeParser = new FundingTreeParser();
    
    // ------------------------ LOGIC --------------------------
    
    @Override
    public Project convert(eu.dnetlib.dhp.schema.oaf.Project sourceProject) throws IOException {
        Preconditions.checkNotNull(sourceProject);

        Project.Builder builder = Project.newBuilder();
        builder.setId(sourceProject.getId());
        Field<String> acronym = sourceProject.getAcronym();
        
        if (isAcronymValid(acronym)) {
            builder.setProjectAcronym(acronym.getValue());
        }
        
        String projectGrantId = sourceProject.getCode() != null ? sourceProject.getCode().getValue() : null;
        if (StringUtils.isNotBlank(projectGrantId)) {
            builder.setProjectGrantId(projectGrantId);
        }
        
        String jsonExtraInfo = sourceProject.getJsonextrainfo() != null ? sourceProject.getJsonextrainfo().getValue() : null;
        if (StringUtils.isNotBlank(jsonExtraInfo)) {
            builder.setJsonextrainfo(jsonExtraInfo);
        } else {
            builder.setJsonextrainfo(BLANK_JSONEXTRAINFO);
        }
        
        String extractedFundingClass = fundingTreeParser.extractFundingClass(
                extractStringValues(sourceProject.getFundingtree()));
        if (StringUtils.isNotBlank(extractedFundingClass)) {
            builder.setFundingClass(extractedFundingClass);
        }
        return isDataValid(builder)?builder.build():null;


    }
    
    /**
     * Verifies whether acronym should be considered as valid.
     * @return true if valid, false otherwise
     */
    public static boolean isAcronymValid(String acronym) {
        return StringUtils.isNotBlank(acronym)
                && !ACRONYM_SKIP_LOWERCASED_VALUES.contains(acronym.trim().toLowerCase());
    }
    
    /**
     * Checks whether Project builder has all required fields set.
     * @return true when all required fields set
     */
    public boolean isDataValid(Project.Builder builder) {
        return builder.hasFundingClass() || builder.hasProjectAcronym() || builder.hasProjectGrantId();
    }
    
    // ------------------------ PRIVATE --------------------------
    
    /**
     * Extracts string values from {@link Field} list.
     */
    private static List<String> extractStringValues(List<Field<String>> source) {
        if (CollectionUtils.isNotEmpty(source)) {
            List<String> results = new ArrayList<String>(source.size());
            for (Field<String> currentField : source) {
                results.add(currentField.getValue());
            }
            return results;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Verifies whether acronym should be considered as valid.
     * @return true if valid, false otherwise
     */
    private static boolean isAcronymValid(Field<String> acronym) {
        return isAcronymValid(acronym != null ? acronym.getValue() : null);
    }

}
