package eu.dnetlib.iis.wf.export.actionmanager;

import eu.dnetlib.iis.common.utils.OaIdUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("deprecation")
public class IdentifierFactoryTest {

    @Test
    public void md5ShouldReturnValueCompatibleWithPersistedIds() {
        // expected value of the persisted project id 40|xyz_________::81dc9bdb52d04dc20036dbd8313ed055
        assertEquals("81dc9bdb52d04dc20036dbd8313ed055", IdentifierFactory.md5("1234"));
    }

    @Test
    public void md5ShouldDelegateToOaIdUtils() {
        assertEquals(OaIdUtils.md5("10.5281/zenodo.1234567"), IdentifierFactory.md5("10.5281/zenodo.1234567"));
        assertEquals(OaIdUtils.md5(""), IdentifierFactory.md5(""));
    }

    @Test
    public void md5ShouldPreserveLegacyNullBehavior() {
        assertNull(IdentifierFactory.md5(null));
    }

}
