package eu.dnetlib.iis.common.string;

/**
 * Operations on {@link CharSequence} 
 * 
 * @author Łukasz Dumiszewski
*/

public final class CharSequenceUtils {

    
    //------------------------ CONSTRUCTORS --------------------------
    
    private CharSequenceUtils() {
        throw new IllegalStateException("may not be initialized");
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Converts the given {@link CharSequence} <code>value</code> to {@link String} by using {@link CharSequence#toString()}.
     * Returns empty string if <code>value</code> is null.
     */
    public static String toStringWithNullToEmpty(CharSequence value) {
        
        return value == null? "": value.toString();
        
    }
    
}
