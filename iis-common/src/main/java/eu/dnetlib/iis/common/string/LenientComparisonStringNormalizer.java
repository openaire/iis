/*
 * This file is part of CoAnSys project.
 * Copyright (c) 2012-2015 ICM-UW
 * 
 * CoAnSys is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * CoAnSys is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
 */
package eu.dnetlib.iis.common.string;

import org.apache.commons.lang3.StringUtils;

/**
 * An implementation of {@link StringNormalizer) that normalizes strings for non-strict comparisons
 * in which one does not care about characters other than letters and digits or about differently written diacritics.
 *
 * @author Łukasz Dumiszewski
 *
 */
public final class LenientComparisonStringNormalizer implements StringNormalizer {

    
    //------------------------ LOGIC --------------------------

    
    
    
    /**
     * Normalizes the given value. <br/>
     * The normalized strings are better suited for non-strict comparisons, in which one does NOT care about characters that are
     * neither letters nor digits; about accidental spaces or different diacritics etc. <br/><br/>
     * This method:
     * <ul>
     * <li>Replaces all characters that are not letters or digits with spaces</li>
     * <li>Replaces white spaces with spaces </li>
     * <li>Trims</li>
     * <li>Compacts multi-space gaps to one-space gaps</li>
     * <li>Removes diacritics</li>
     * <li>Changes characters to lower case</li>
     * </ul>
     * Returns "" if the passed value is null or blank
     *
     * @param value the string to normalize 
     * @see DiacriticsRemover#removeDiacritics(String, boolean)
     *
     *
     */
    public String normalize(String value) {
        
        if (StringUtils.isBlank(value)) {
        
            return "";

        }
        
        
        String result = null;
        
        result = removeNonLetterDigitCharacters(value);
        
        result = DiacriticsRemover.removeDiacritics(result);
        
        result = result.toLowerCase();
        
        result = result.trim().replaceAll(" +", " ");
        
        return result;
    }
    
    
    
    
    //------------------------ PRIVATE --------------------------

    
    private String removeNonLetterDigitCharacters(final String value) {
        
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < value.length(); ++i) {
   
            char c = value.charAt(i);
            
            if (Character.isLetterOrDigit(c)) {
                sb.append(c);
            } else {
                sb.append(" ");
            }
        }
        
        return sb.toString();
    }

 

}
