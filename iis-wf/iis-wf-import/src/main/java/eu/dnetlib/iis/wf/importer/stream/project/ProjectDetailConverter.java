package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.infospace.converter.FundingTreeParser;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;

/**
 * Utility class for converting projects into different representation. 
 * @author mhorst
 *
 */
public class ProjectDetailConverter {
    
    private final FundingTreeParser fundingTreeParser = new FundingTreeParser();

    /**
     * Converts {@link ProjectDetail} into {@link Project}.
     */
    public Project convert(ProjectDetail source) throws IOException {
        Project.Builder builder = Project.newBuilder();
        
        builder.setId(normalizeId(source.getProjectId()));
        
        if (ProjectConverter.isAcronymValid(source.getAcronym())) {
            builder.setProjectAcronym(source.getAcronym());
        }

        if (StringUtils.isNotBlank(source.getCode())) {
            builder.setProjectGrantId(source.getCode());
        }
        
        if (StringUtils.isNotBlank(source.getJsonextrainfo())) {
            builder.setJsonextrainfo(source.getJsonextrainfo());
        } else {
            builder.setJsonextrainfo(ProjectConverter.BLANK_JSONEXTRAINFO);
        }
        
        if (CollectionUtils.isNotEmpty(source.getFundingPath())) {
            builder.setFundingClass(fundingTreeParser.extractFundingClass(source.getFundingPath())); 
        }

        return builder.build();
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Normalizes indetifier into OpenAIRE main identifier format.
     */
    private static String normalizeId(String sourceId) {
        String[] tokenizedProjectId = StringUtils.splitByWholeSeparator(
                sourceId, InfoSpaceConstants.ID_NAMESPACE_SEPARATOR);
        if (tokenizedProjectId==null || tokenizedProjectId.length!=2) {
            throw new RuntimeException("unexpected projectId format: " + sourceId + 
                    ", unable to split into two by " + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR);
        }
        return projectOafId(tokenizedProjectId[0], tokenizedProjectId[1]);
    }

    private static String projectOafId(String prefix, String id) {
        if (id.isEmpty() || prefix.isEmpty()) {
            return "";
        }
        return projectOafSimpleId(prefix + "::" + DigestUtils.md5Hex(id));
    }

    private static String projectOafSimpleId(String id) {
        return (InfoSpaceConstants.ROW_PREFIX_PROJECT + id).replaceAll("\\s|\\n", "");
    }
}
