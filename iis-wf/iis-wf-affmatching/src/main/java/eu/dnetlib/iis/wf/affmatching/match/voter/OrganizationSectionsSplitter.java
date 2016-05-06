package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Splitter of organization name into sections.
 * 
 * @author madryk
 */
class OrganizationSectionsSplitter {

    private static final List<String> RESTRICTED_SECTIONS = ImmutableList.<String>builder()
            .add("ltd")
            .add("inc")
            .build();
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private OrganizationSectionsSplitter() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static List<String> splitToSections(String organizationName) {
        
        String[] sectionsArray = StringUtils.split(organizationName, ",;");
        
        List<String> sections = Lists.newArrayList();
        
        
        for (int i=0; i<sectionsArray.length; ++i) {
            
            String section = sectionsArray[i].trim();
            
            if (StringUtils.isNotBlank(section) && !RESTRICTED_SECTIONS.contains(section)) {
                sections.add(section);
            }
        }
        
        return sections;
    }
}
