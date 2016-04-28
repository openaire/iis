package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
* @author Łukasz Dumiszewski
*/

public class AffOrgMatchVoterStrengthCalculatorTest {

    
    private AffOrgMatchVoterStrengthCalculator calculator = new AffOrgMatchVoterStrengthCalculator();
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = IllegalArgumentException.class)
    public void calculateStrength_position_less_than_zero() {
        
        // execute
        
        calculator.calculateStrength(-1, 4);
        
    }
    

    @Test(expected = IllegalArgumentException.class)
    public void calculateStrength_voters_less_than_one() {
        
        // execute
        
        calculator.calculateStrength(1, 0);
        
    }

    
    @Test(expected = IllegalArgumentException.class)
    public void calculateStrength_position_bigger_than_voters() {
        
        // execute
        
        calculator.calculateStrength(1, 1);
        
    }
    
    
    @Test
    public void calculateStrength() {
        
        // execute & assert
        
        assertEquals(1, calculator.calculateStrength(0, 1));
        
        assertEquals(2, calculator.calculateStrength(0, 2));
        assertEquals(1, calculator.calculateStrength(1, 2));
        
        assertEquals(4, calculator.calculateStrength(0, 3));
        assertEquals(2, calculator.calculateStrength(1, 3));
        assertEquals(1, calculator.calculateStrength(2, 3));

        assertEquals(8, calculator.calculateStrength(0, 4));
        assertEquals(4, calculator.calculateStrength(1, 4));
        assertEquals(2, calculator.calculateStrength(2, 4));
        assertEquals(1, calculator.calculateStrength(3, 4));

        assertEquals(16, calculator.calculateStrength(0, 5));
        assertEquals(8, calculator.calculateStrength(1, 5));
        assertEquals(4, calculator.calculateStrength(2, 5));
        assertEquals(2, calculator.calculateStrength(3, 5));
        assertEquals(1, calculator.calculateStrength(4, 5));
        
        assertEquals(32, calculator.calculateStrength(0, 6));
        assertEquals(16, calculator.calculateStrength(1, 6));
        assertEquals(8, calculator.calculateStrength(2, 6));
        assertEquals(4, calculator.calculateStrength(3, 6));
        assertEquals(2, calculator.calculateStrength(4, 6));
        assertEquals(1, calculator.calculateStrength(5, 6));

    }
    
    
}
