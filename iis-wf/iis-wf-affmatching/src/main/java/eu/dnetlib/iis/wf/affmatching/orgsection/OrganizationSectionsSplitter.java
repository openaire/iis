package eu.dnetlib.iis.wf.affmatching.orgsection;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection.OrgSectionType;

/**
 * Splitter of organization name into sections.
 * 
 * @author madryk
 */
public class OrganizationSectionsSplitter implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private static final List<String> RESTRICTED_SECTIONS = ImmutableList.<String>builder()
            .add("ltd")
            .add("inc")
            .build();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Splits provided organization name into sections.<br/>
     * Method assumes that sections are separated by a comma or by a semicolon in
     * organization name string.
     */
    public List<String> splitToSections(String organizationName) {
        
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
    
    /**
     * Splits provided organization name into sections.<br/>
     * Internally uses {@link #splitToSections(String)}.
     * 
     * @return detailed information about sections
     */
    public List<OrganizationSection> splitToSectionsDetailed(String organizationName) {
        
        List<String> sections = splitToSections(organizationName);
        List<OrganizationSection> sectionsDetailed = Lists.newArrayList();
        
        for (String section : sections) {
            
            OrganizationSection sectionDetailed = buildSectionDetailed(section);
            
            sectionsDetailed.add(sectionDetailed);
        }
        
        return sectionsDetailed;
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private OrganizationSection buildSectionDetailed(String section) {
        
        String[] sectionWords = section.split(" ");
        
        
        int universityWordPos = findWordStartingWithAny(sectionWords, "univ", "uniw");
        
        if (universityWordPos != -1) {
            return new OrganizationSection(OrgSectionType.UNIVERSITY, sectionWords, universityWordPos);
        }
        
        
        return new OrganizationSection(OrgSectionType.UNKNOWN, sectionWords, -1);
    }
    
    private int findWordStartingWithAny(String[] words, String ... wordStart) {
        
        for (int i=0; i<words.length; ++i) {
            for (int j=0; j<wordStart.length; ++j) {
                
                if (words[i].startsWith(wordStart[j])) {
                    return i;
                }
            }
        }
        return -1;
    }
}
