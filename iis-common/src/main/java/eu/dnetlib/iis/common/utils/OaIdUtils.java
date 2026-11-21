package eu.dnetlib.iis.common.utils;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility methods for OpenAIRE identifier generation shared by the importer and exporter modules.
 */
public class OaIdUtils {

    private OaIdUtils() {
    }

    /**
     * Computes the lowercase hexadecimal MD5 digest of the given string (UTF-8 encoded). This is the single
     * implementation used wherever an MD5 hash is needed for OpenAIRE id generation purposes, so that generated
     * identifiers remain compatible with already persisted records.
     *
     * @param s the value to hash
     * @return the 32 character lowercase hex MD5 digest
     * @throws NullPointerException if {@code s} is null (fail fast; the deprecated IdentifierFactory#md5 keeps the
     * legacy null-returning behavior for backward compatibility, and no id generation call site passes null)
     */
    public static String md5(final String s) {
        return DigestUtils.md5Hex(s);
    }

}
