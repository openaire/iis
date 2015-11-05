package eu.dnetlib.iis.common.spark.test;

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
        
        SparkJobBuilder retBuilder = builder.setMaster("local[3]");
        
        // assert
        
        assertTrue(retBuilder == builder);
        verify(sparkJob).setMaster("local[3]");
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void setAppName() {
        
        // execute
        
        SparkJobBuilder retBuilder = builder.setAppName("AppName");
        
        // assert
        
        assertTrue(retBuilder == builder);
        verify(sparkJob).setAppName("AppName");
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void setMainClass() {
        
        // execute
        
        SparkJobBuilder retBuilder = builder.setMainClass(this.getClass());
        
        // assert
        
        assertTrue(retBuilder == builder);
        verify(sparkJob).setMainClass(this.getClass());
        verifyNoMoreInteractions(sparkJob);
    }
    
    
    @Test
    public void addArg() {
    
        // execute
        
        SparkJobBuilder retBuilder = builder.addArg("argName", "argValue");


        // assert
        
        assertTrue(retBuilder == builder);
        verify(sparkJob).addArg("argName", "argValue");
        verifyNoMoreInteractions(sparkJob);

    }
    
    @Test
    public void setArgNameValueSeparator() {
    
        // execute
        
        SparkJobBuilder retBuilder = builder.setArgNameValueSeparator(":");


        // assert
        
        assertTrue(retBuilder == builder);
        verify(sparkJob).setArgNameValueSeparator(":");
        verifyNoMoreInteractions(sparkJob);

    }
    
    @Test
    public void addJobProperty() {
    
        // execute
        
        SparkJobBuilder retBuilder = builder.addJobProperty("propName", "propValue");


        // assert
        
        assertTrue(retBuilder == builder);
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
