package eu.dnetlib.iis.common;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * @author Łukasz Dumiszewski
 */

public class TestsIOUtilsTest {
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void assertUtf8TextContentsEqual_EQUAL() {
        
        // given
        String str0 = "Ala ma kota\r\nKot ma Alę\n";
        String str1 = "Ala ma kota\r\nKot ma Alę\n";
        
        
        // assert
        TestsIOUtils.assertUtf8TextContentsEqual(IOUtils.toInputStream(str0), IOUtils.toInputStream(str1));
        
    }
    
    @Test
    public void assertUtf8TextContentsEqual_NEW_LINES_DIFF() {
        
        // given
        String str0 = "Ala ma kota\nKot ma Alę\n";
        String str1 = "Ala ma kota\r\nKot ma Alę\n";
        
        
        // assert
        TestsIOUtils.assertUtf8TextContentsEqual(IOUtils.toInputStream(str0), IOUtils.toInputStream(str1));
        
    }
    
    @Test(expected = AssertionError.class)
    public void assertUtf8TextContentsEqual_TEXT_DIFF() {
        
        // given
        String str0 = "Ala ma kot\nKot ma Alę\n";
        String str1 = "Ala ma kota\r\nKot ma Alę\n";
        
        
        // assert
        TestsIOUtils.assertUtf8TextContentsEqual(IOUtils.toInputStream(str0), IOUtils.toInputStream(str1));
        
    }
    
    @Test(expected = AssertionError.class)
    public void assertUtf8TextContentsEqual_DIFF_LINE_COUNT() {
        
        // given
        String str0 = "Ala ma kota\nKot ma Alę\n";
        String str1 = "Ala ma kota\r\nKot ma Alę\nAdditional line\n";
        
        
        // assert
        TestsIOUtils.assertUtf8TextContentsEqual(IOUtils.toInputStream(str0), IOUtils.toInputStream(str1));
        
    }
    
}
