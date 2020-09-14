package eu.dnetlib.iis.common.string;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Common string utils.
 */
public class StringUtils {

    /**
     * Decodes an UTF-8 encoded string.
     *
     * @param source UTF-8 encoded string.
     * @return Decoded string.
     */
    public static String decodeFromUtf8(String source) {
        try {
            return URLDecoder.decode(source, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
