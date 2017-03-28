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

import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 * {@link OafEntity} containing project details to {@link Project} converter.
 * 
 * @author mhorst
 *
 */
public class ProjectConverter implements OafEntityToAvroConverter<Project> {

    protected static final Logger log = Logger.getLogger(ProjectConverter.class);

    public static final String BLANK_JSONEXTRAINFO = "{}";

    private static final Set<String> ACRONYM_SKIP_LOWERCASED_VALUES = new HashSet<String>(
            Arrays.asList("undefined", "unknown"));
    
    private final FundingTreeParser fundingTreeParser = new FundingTreeParser();
    
    // ------------------------ LOGIC --------------------------
    
    @Override
    public Project convert(OafEntity oafEntity) throws IOException {
        Preconditions.checkNotNull(oafEntity);
        if (oafEntity.hasProject()) {
            eu.dnetlib.data.proto.ProjectProtos.Project sourceProject = oafEntity.getProject();
            if (sourceProject.hasMetadata()) {
                Project.Builder builder = Project.newBuilder();
                builder.setId(oafEntity.getId());
                StringField acronym = sourceProject.getMetadata().getAcronym();
                
                if (isAcronymValid(acronym)) {
                    builder.setProjectAcronym(acronym.getValue());
                }
                
                String projectGrantId = sourceProject.getMetadata().getCode().getValue();
                if (StringUtils.isNotBlank(projectGrantId)) {
                    builder.setProjectGrantId(projectGrantId);
                }
                
                String jsonExtraInfo = sourceProject.getMetadata().getJsonextrainfo().getValue();
                if (StringUtils.isNotBlank(jsonExtraInfo)) {
                    builder.setJsonextrainfo(jsonExtraInfo);
                } else {
                    builder.setJsonextrainfo(BLANK_JSONEXTRAINFO);
                }
                
                String extractedFundingClass = fundingTreeParser.extractFundingClass(
                        extractStringValues(sourceProject.getMetadata().getFundingtreeList()));
                if (StringUtils.isNotBlank(extractedFundingClass)) {
                    builder.setFundingClass(extractedFundingClass);
                }
                return isDataValid(builder)?builder.build():null;
            } else {
                log.error("skipping: no metadata for project " + oafEntity.getId());
                return null;
            }
        } else {
            log.error("skipping: no project for entity " + oafEntity.getId());
            return null;
        }
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
     * Extracts string values from {@link StringField} list.
     */
    private static List<String> extractStringValues(List<StringField> source) {
        if (CollectionUtils.isNotEmpty(source)) {
            List<String> results = new ArrayList<String>(source.size());
            for (StringField currentField : source) {
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
    private static boolean isAcronymValid(StringField acronym) {
        return isAcronymValid(acronym.getValue());
    }

}
