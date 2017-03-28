package eu.dnetlib.iis.wf.affmatching.orgsection;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Class providing detailed information about organization section
 * 
 * @author madryk
 */
public class OrganizationSection implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Number of first letters of {@link OrgSectionType#name()} that
     * ensures uniqueness among other section types.
     */
    public static final int SECTION_NUMBER_OF_LETTERS = 3;

    public enum OrgSectionType {
        UNIVERSITY,
        UNKNOWN
    }
    
    private final OrgSectionType type;
    
    private final String[] sectionWords;
    
    private final int typeSignificantWordPos;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param type - recognized type ({@link OrgSectionType#UNKNOWN} if type was not recognized)
     * @param sectionWords - table of words in section
     * @param typeSignificantWordPos - position of word which was crucial
     *      to determine type of section (should be -1 for {@link OrgSectionType#UNKNOWN})
     */
    public OrganizationSection(OrgSectionType type, String[] sectionWords, int typeSignificantWordPos) {
        Preconditions.checkArgument(
                (typeSignificantWordPos >= 0 && type != OrgSectionType.UNKNOWN) || 
                (typeSignificantWordPos == -1 && type == OrgSectionType.UNKNOWN));
        Preconditions.checkArgument(typeSignificantWordPos < sectionWords.length);
        
        this.type = type;
        this.sectionWords = sectionWords;
        this.typeSignificantWordPos = typeSignificantWordPos;
    }


    //------------------------ GETTERS --------------------------
    
    /**
     * Returns type of this section
     */
    public OrgSectionType getType() {
        return type;
    }
    
    /**
     * Returns all words of this section
     */
    public String[] getSectionWords() {
        return sectionWords;
    }
    
    /**
     * Returns position of the word in {@link #getSectionWords()} which
     * was crucial to determine the type of this section.
     * If there were not such a word then -1 will be returned.
     */
    public int getTypeSignificantWordPos() {
        return typeSignificantWordPos;
    }


    //------------------------ hashCode & equals --------------------------
    
    @Override
    public int hashCode() {
        return Objects.hashCode(type, sectionWords, typeSignificantWordPos);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final OrganizationSection other = (OrganizationSection) obj;
        return Objects.equal(this.type, other.type) &&
                Arrays.equals(this.sectionWords, other.sectionWords) &&
                Objects.equal(this.typeSignificantWordPos, other.typeSignificantWordPos);
    }


    //------------------------ toString --------------------------
    
    @Override
    public String toString() {
        return "OrganizationSection [type=" + type + ", sectionWords=" + Arrays.toString(sectionWords)
                + ", typeSignificantWordPos=" + typeSignificantWordPos + "]";
    }
    
    
}
