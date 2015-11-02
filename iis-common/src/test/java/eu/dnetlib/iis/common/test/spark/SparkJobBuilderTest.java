package eu.dnetlib.iis.common.test.spark;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * @author Łukasz Dumiszewski
 */

public class SparkJobBuilderTest {

    private SparkJobBuilder builder = SparkJobBuilder.create();
    
    private SparkJob sparkJob = Mockito.mock(SparkJob.class);
    
    
    
    @Before
    public void before() {
    
        Whitebox.setInternalState(builder, "sparkJob", sparkJob);
    
    }
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void setMaster() {
        
        // execute
        
        builder.setMaster("local[3]");
        
        // assert
        
        verify(sparkJob).setMaster("local[3]");
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void setAppName() {
        
        // execute
        
        builder.setAppName("AppName");
        
        // assert
        
        verify(sparkJob).setAppName("AppName");
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void setMainClass() {
        
        // execute
        
        builder.setMainClass(this.getClass());
        
        // assert
        
        verify(sparkJob).setMainClass(this.getClass());
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void addArg() {
    
        // execute
        
        builder.addArg("argName", "argValue");


        // assert
        
        verify(sparkJob).addArg("argName", "argValue");
        verifyNoMoreInteractions(sparkJob);

    }
    
    @Test
    public void setArgNameValueSeparator() {
    
        // execute
        
        builder.setArgNameValueSeparator(":");


        // assert
        
        verify(sparkJob).setArgNameValueSeparator(":");
        verifyNoMoreInteractions(sparkJob);

    }
    
    @Test
    public void addJobProperty() {
    
        // execute
        
        builder.addJobProperty("propName", "propValue");


        // assert
        
        verify(sparkJob).addJobProperty("propName", "propValue");
        verifyNoMoreInteractions(sparkJob);

    }
    
    @Test(expected=NullPointerException.class)
    public void build_NullMainClass() {
    
        // execute
        
        builder.build();

    }
    
    @Test
    public void build() {
    
        // given
        Mockito.doReturn(this.getClass()).when(sparkJob).getMainClass();
        
        // execute
        
        SparkJob builtSparkJob = builder.build();

        // assert
        assertTrue(builtSparkJob == sparkJob);
        
    }

}
