package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Class that generates hash of the given string. 
 * 
 * @author Łukasz Dumiszewski
*/

class StringPartFirstLettersHasher implements Serializable {

    
    private static final long serialVersionUID = 1L;

    private int numberOfParts = 2;
    
    private int numberOfLettersPerPart = 2;
    

    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns a hash of the given value. The hash consists of {@link #setNumberOfLettersPerPart(int)} first letters
     * from {@link #setNumberOfParts(int)} first parts of the given value. A part is a word separated from other words with
     * white-space(s). If a part has less letters than {@link #setNumberOfLettersPerPart(int)} then it is completed with '_'.
     * If the number of parts in the value is smaller than {@link #setNumberOfParts(int)} then the hash will take into
     * account less parts.<br/>
     * Examples:<br/>
     * <code>
     * numberOfParts = 3, numberOfLetters = 3 <br/>
     * hash("Alice has a cat") => Alihasa__ <br/>
     * hash("Alice is") => Aliis_ <br/>
     * </code> 
     */
    public String hash(String value) {
        
        if (StringUtils.isBlank(value)) return "";
        
        
        value = value.trim();
        
        String[] parts = value.split("\\s+");
        
        int realNumberOfParts = this.numberOfParts;
        
        if (parts.length < this.numberOfParts) {
            realNumberOfParts = parts.length;
        }

        return generateHash(realNumberOfParts, this.numberOfLettersPerPart, parts);
    }

    
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private String generateHash(int numberOfParts, int numberOfLettersPerPart, String[] parts) {
    
        StringBuilder hash = new StringBuilder();
        
        for (int i = 0; i< numberOfParts; i++) {
            String part = parts[i];
            part = StringUtils.rightPad(part, numberOfLettersPerPart, "_");
            hash.append(part.substring(0, numberOfLettersPerPart));
        }
        
        return hash.toString();
    }


    
    //------------------------ SETTERS --------------------------

    /**
     * Sets number of parts of the hashed string that will be taken into account in the hash generating algorithm.
     * 2 if not set.
     * @see #hash(String)
     */
    public void setNumberOfParts(int numberOfParts) {
        
        Preconditions.checkArgument(numberOfParts >= 1);
        
        this.numberOfParts = numberOfParts;
    
    }

    /**
     * Sets number of letters of each part of the hashed string that will be taken into account in the hash generating algorithm.
     * 2 if not set.
     * @see #hash(String)
     */
    public void setNumberOfLettersPerPart(int numberOfLettersPerPart) {
        
        Preconditions.checkArgument(numberOfLettersPerPart >= 1);
        
        this.numberOfLettersPerPart = numberOfLettersPerPart;
        
    }
    
    
   
}
