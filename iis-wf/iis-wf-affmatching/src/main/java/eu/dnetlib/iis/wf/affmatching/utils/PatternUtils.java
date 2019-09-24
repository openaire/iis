package eu.dnetlib.iis.wf.affmatching.utils;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Class encapsulating pattern related utility methods.
 * 
 * @author mhorst
 *
 */
public class PatternUtils {

    /**
     * Removes each substring of the source String that matches the given regular expression using the DOTALL option.
     * @param source the source string
     * @param regex the regular expression to which this string is to be matched
     * @return The resulting {@code String}
     */
    public static String removePattern(final String source, final String regex) {
//      FIXME reintroduce StringUtils.removePattern() reference as soon as the dependency clash is resolved, more details in:
//      https://github.com/openaire/iis/issues/987#issuecomment-534500354
//      return StringUtils.removePattern(source, regex);
        return Pattern.compile(regex, Pattern.DOTALL).matcher(source).replaceAll(StringUtils.EMPTY);
    }
}
