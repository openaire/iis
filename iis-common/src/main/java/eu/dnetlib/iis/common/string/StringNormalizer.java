package eu.dnetlib.iis.common.string;

/**
 * String normalizer.
 *
 * @author Łukasz Dumiszewski
 *
 */
public interface StringNormalizer {
    
    /**
     * Normalizes the given string value.
     */
    public String normalize(String value);
    
}