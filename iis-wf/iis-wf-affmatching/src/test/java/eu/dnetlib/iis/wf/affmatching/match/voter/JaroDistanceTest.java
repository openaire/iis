package eu.dnetlib.iis.wf.affmatching.match.voter;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
* @author Łukasz Dumiszewski
*/

public class JaroDistanceTest {

    
    /**@Test
    public void test() {
        
        for (int i = 0 ; i < 1000; i++) {
            String a = RandomStringUtils.random(i % 50);
            String b = RandomStringUtils.randomAlphanumeric(i % 33);
            
            System.out.println("a="+a+", b="+b);
            assertEquals(StringUtils.getJaroWinklerDistance(a, b), StringUtils.getJaroWinklerDistance(b, a), 0.0000001);
        }
    }*/
    
    @Test
    public void test2() {
        
        CommonSimilarWordCalculator calculator = new CommonSimilarWordCalculator(0.9);
        
        List<String> affWords = Lists.newArrayList("universite", "paris", "curie", "marie", "laboratoire", "oceanographie", "villefranche", "universites", "sorbonne", "pierre");
        List<String> orgWords = Lists.newArrayList("universitetet", "bergen");
        
        System.out.println(calculator.calcSimilarWordNumber(affWords, orgWords));
        System.out.println(calculator.calcSimilarWordRatio(affWords, orgWords) + "\n");
        System.out.println(calculator.calcSimilarWordNumber(orgWords, affWords));
        System.out.println(calculator.calcSimilarWordRatio(orgWords, affWords));
        
    }
    
    
}
