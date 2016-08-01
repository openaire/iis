package eu.dnetlib.iis.wf.affmatching.match.voter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class JaroWinklerDistanceTest {

    private double EPSILON = 0.001;
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void getDistance() {
        
        // assert
        assertEquals(0.855, JaroWinklerDistanceCalculator.getDistance("university", "uniwersytet"), EPSILON);
        assertEquals(0.957, JaroWinklerDistanceCalculator.getDistance("university", "universidad"), EPSILON);
        assertEquals(0.949, JaroWinklerDistanceCalculator.getDistance("warszawa", "warsaw"), EPSILON);
        assertEquals(0.583, JaroWinklerDistanceCalculator.getDistance("warszawa", "bieniawa"), EPSILON);
        assertEquals(0, JaroWinklerDistanceCalculator.getDistance("warsaw", "sydney"), EPSILON);
        
    }
    
    
}
