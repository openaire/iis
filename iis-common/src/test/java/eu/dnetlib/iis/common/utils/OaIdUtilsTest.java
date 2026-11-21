package eu.dnetlib.iis.common.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OaIdUtilsTest {

    @Test
    public void md5ShouldMatchDigestUtilsMd5Hex() {
        assertEquals(DigestUtils.md5Hex("1234"), OaIdUtils.md5("1234"));
        assertEquals(DigestUtils.md5Hex(""), OaIdUtils.md5(""));
        assertEquals(DigestUtils.md5Hex("ünïcodé ✓"), OaIdUtils.md5("ünïcodé ✓"));
    }

    @Test
    public void md5ShouldReturnExpectedValuesCompatibleWithPersistedIds() {
        // expected value of the persisted project id 40|xyz_________::81dc9bdb52d04dc20036dbd8313ed055
        assertEquals("81dc9bdb52d04dc20036dbd8313ed055", OaIdUtils.md5("1234"));
        // lens id used by the document-to-patent exporter
        assertEquals("5f9cd4d04236b859d589e9611b005a91", OaIdUtils.md5("lensId"));
        // doi-like pid value
        assertEquals("cb8d6e805d0416f65b769cdfcf3a1f3c", OaIdUtils.md5("10.5281/zenodo.1234567"));
    }

    @Test
    public void md5ShouldHandleEdgeCases() {
        // empty string
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", OaIdUtils.md5(""));
        // non-ascii input is hashed as UTF-8
        assertEquals("4529988ed3cc89700bc8fd594cfec627", OaIdUtils.md5("ünïcodé ✓"));
        // result is always a 32 char lowercase hex string
        String digest = OaIdUtils.md5("some value with whitespace \n and\ttabs");
        assertEquals(32, digest.length());
        assertEquals(digest, digest.toLowerCase());
    }

}
