package eu.dnetlib.iis.wf.affmatching.bucket;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

/**
* @author Łukasz Dumiszewski
*/

class StringPartFirstLettersHasher implements Serializable {

    
    private static final long serialVersionUID = 1L;

    
    
    //------------------------ LOGIC --------------------------
    
    

    public String hash(String value, int numberOfParts, int numberOfLettersPerPart) {
        
        Preconditions.checkArgument(numberOfParts >= 1);
        
        Preconditions.checkArgument(numberOfLettersPerPart >= 1);
        
        
        if (StringUtils.isBlank(value)) return "";
        
        
        value = value.trim();
        
        String[] parts = value.split("\\s+");
        
        if (parts.length < numberOfParts) {
            numberOfParts = parts.length;
        }

        return generateHash(numberOfParts, numberOfLettersPerPart, parts);
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
    
    
   
}
