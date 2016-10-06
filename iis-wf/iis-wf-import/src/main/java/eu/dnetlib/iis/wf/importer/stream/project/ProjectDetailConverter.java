package eu.dnetlib.iis.wf.importer.stream.project;

import java.io.IOException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;
import eu.dnetlib.openaire.exporter.model.ProjectDetail;

/**
 * Utility class for converting projects into different representation. 
 * @author mhorst
 *
 */
public class ProjectDetailConverter {

    /**
     * Converts {@link ProjectDetail} into {@link Project}.
     */
    public static Project convert(ProjectDetail source) throws IOException {
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
            builder.setFundingClass(ProjectConverter.extractFundingClass(source.getFundingPath())); 
        }

        return builder.build();
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Normalizes indetifier into OpenAIRE main identifier format.
     */
    private static String normalizeId(String sourceId) {
        String[] tokenizedProjectId = StringUtils.splitByWholeSeparator(
                sourceId, HBaseConstants.ID_NAMESPACE_SEPARATOR);
        if (tokenizedProjectId==null || tokenizedProjectId.length!=2) {
            throw new RuntimeException("unexpected projectId format: " + sourceId + 
                    ", unable to split into two by " + HBaseConstants.ID_NAMESPACE_SEPARATOR);
        }
        return AbstractDNetXsltFunctions.oafId(Type.project.name(), 
                tokenizedProjectId[0], tokenizedProjectId[1]); 
    }
    
}
