package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSection;

/**
 * Class that generates hash of the given {@link OrganizationSection}.
 * 
 * @author madryk
 */
public class OrganizationSectionHasher implements Serializable {

    private static final long serialVersionUID = 1L;
    
    
    private int numberOfLettersPerWord = 3;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the passed section.<br/>
     * 
     * Hash is generated from first letters of {@link OrganizationSection#getType()},
     * first {@link OrganizationSectionHasher#setNumberOfLettersPerWord(int)} letters
     * of one word before and one word after the type significant word 
     * {@link OrganizationSection#getTypeSignificantWordPos()}.<br/>
     * 
     * If passed section has no type significant word then 
     * the first and the second word of the section will be used.<br/>
     * 
     * Hash length is always the same. Any missing characters will be replaced
     * by underscores.<br/><br/>
     * 
     */
    public String hash(OrganizationSection section) {
        
        if (section.getTypeSignificantWordPos() == -1) {
            return hashUsingFirstWords(section);
        }
        
        return hashUsingAdjacentWords(section);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private String hashUsingAdjacentWords(OrganizationSection section) {
        
        String[] words = section.getSectionWords();
        int typeSignificantWordPos = section.getTypeSignificantWordPos();
        
        String wordBefore = (typeSignificantWordPos <= 0) ? "" : words[typeSignificantWordPos - 1];
        String wordAfter = (typeSignificantWordPos >= words.length-1) ? "" : words[typeSignificantWordPos + 1];
        
        return generateWordHash(section.getType().name(), OrganizationSection.SECTION_NUMBER_OF_LETTERS) 
                + generateWordHash(wordBefore) + generateWordHash(wordAfter);
    }
    
    private String hashUsingFirstWords(OrganizationSection section) {
        
        String[] words = section.getSectionWords();
        
        String firstWord = words.length == 0 ? "" : words[0];
        String secondWord = words.length <= 1 ? "" : words[1];
        
        return generateWordHash(section.getType().name(), OrganizationSection.SECTION_NUMBER_OF_LETTERS) 
                + generateWordHash(firstWord) + generateWordHash(secondWord);
    }
    
    private String generateWordHash(String word) {
        return generateWordHash(word, numberOfLettersPerWord);
    }
    
    private String generateWordHash(String word, int numberOfLettersPerWord) {
        return StringUtils.rightPad(word, numberOfLettersPerWord, '_').substring(0, numberOfLettersPerWord);
    }


    //------------------------ SETTERS --------------------------
    
    public void setNumberOfLettersPerWord(int numberOfLettersPerWord) {
        this.numberOfLettersPerWord = numberOfLettersPerWord;
    }
    
}
