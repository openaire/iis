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

import java.text.Normalizer;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapping to the basic Latin alphabet (a-z, A-Z). In most cases, a character is
 * mapped to the closest visual form, rather than functional one, e.g.: "ö" is
 * mapped to "o" rather than "oe", and "đ" is mapped to "d" rather than "dj" or
 * "gj". Notable exceptions include: "ĸ" mapped to "q", "ß" mapped to "ss", and
 * "Þ", "þ" mapped to "Y", "y".
 *
 * <p> Each character is processed as follows: <ol> <li>the character is
 * compatibility decomposed,</li> <li>all the combining marks are removed,</li>
 * <li>the character is compatibility composed,</li> <li>additional "manual"
 * substitutions are applied.</li> </ol> </p>
 *
 * <p> All the characters from the "Latin-1 Supplement" and "Latin Extended-A"
 * Unicode blocks are mapped to the "Basic Latin" block. Characters from other
 * alphabets are generally left intact, although the decomposable ones may be
 * affected by the procedure. </p>
 *
 * @author Lukasz Bolikowski (bolo@icm.edu.pl)
 * 
 * @author Łukasz Dumiszewski /just copied from coansys-commons/
 *
 */
public final class DiacriticsRemover {

    private DiacriticsRemover() {}
    
    
    private static final Character[] from = {
        'Æ', 'Ð', 'Ø', 'Þ', 'ß', 'æ', 'ð', 'ø', 'þ', 'Đ', 'đ', 'Ħ',
        'ħ', 'ı', 'ĸ', 'Ł', 'ł', 'Ŋ', 'ŋ', 'Œ', 'œ', 'Ŧ', 'ŧ'};
    private static final String[] to = {
        "AE", "D", "O", "Y", "ss", "ae", "d", "o", "y", "D", "d", "H",
        "h", "i", "q", "L", "l", "N", "n", "OE", "oe", "T", "t"};
    
    private static Map<Character, String> lookup = buildLookup();
    
    

    //------------------------ LOGIC --------------------------
    
    
    /**
     * Removes diacritics from a text.
     *
     * @param text Text to process.
     * @return Text without diacritics.
     */
    public static String removeDiacritics(String text) {
        if (text == null) {
            return null;
        }

        String tmp = Normalizer.normalize(text, Normalizer.Form.NFKD);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tmp.length(); i++) {
            Character ch = tmp.charAt(i);
            if (Character.getType(ch) == Character.NON_SPACING_MARK) {
                continue;
            }

            if (lookup.containsKey(ch)) {
                builder.append(lookup.get(ch));
            } else {
                builder.append(ch);
            }
        }

        return builder.toString();
    }


    //------------------------ PRIVATE --------------------------
    
    private static Map<Character, String> buildLookup() {
        if (from.length != to.length) {
            throw new IllegalStateException();
        }

        Map<Character, String> _lookup = new HashMap<Character, String>();
        for (int i = 0; i < from.length; i++) {
            _lookup.put(from[i], to[i]);
        }

        return _lookup;
    }
}
