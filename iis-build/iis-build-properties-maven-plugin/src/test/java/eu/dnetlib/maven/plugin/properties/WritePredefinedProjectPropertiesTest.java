package eu.dnetlib.maven.plugin.properties;

import static eu.dnetlib.maven.plugin.properties.WritePredefinedProjectProperties.PROPERTY_PREFIX_ENV;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class WritePredefinedProjectPropertiesTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Mock
    private MavenProject mavenProject;
    
    private WritePredefinedProjectProperties mojo;

    @Before
    public void init() {
        mojo = new WritePredefinedProjectProperties();
        mojo.outputFile = getPropertiesFileLocation();
        mojo.project = mavenProject;
        doReturn(new Properties()).when(mavenProject).getProperties();
    }

    // ----------------------------------- TESTS ---------------------------------------------
    
    @Test
    public void testExecuteEmpty() throws Exception {
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(0, storedProperties.size());
    }
    
    @Test
    public void testExecuteWithProjectProperties() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(key));
        assertEquals(value, storedProperties.getProperty(key));
    }
    
    @Test(expected=MojoExecutionException.class)
    public void testExecuteWithProjectPropertiesAndInvalidOutputFile() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        doReturn(projectProperties).when(mavenProject).getProperties();
        mojo.outputFile = testFolder.getRoot();
        
        // execute
        mojo.execute();
    }
    
    @Test
    public void testExecuteWithProjectPropertiesExclusion() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String excludedKey = "excludedPropertyKey";
        String excludedValue = "excludedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(excludedKey, excludedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        mojo.setExclude(excludedKey);
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(key));
        assertEquals(value, storedProperties.getProperty(key));
    }
    
    @Test
    public void testExecuteWithProjectPropertiesInclusion() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        mojo.setInclude(includedKey);
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(includedKey));
        assertEquals(includedValue, storedProperties.getProperty(includedKey));
    }
    
    @Test
    public void testExecuteIncludingPropertyKeysFromFile() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        File includedPropertiesFile = new File(testFolder.getRoot(), "included.properties");
        Properties includedProperties = new Properties();
        includedProperties.setProperty(includedKey, "irrelevantValue");
        includedProperties.store(new FileWriter(includedPropertiesFile), null);
        
        mojo.setIncludePropertyKeysFromFiles(new String[] {includedPropertiesFile.getAbsolutePath()});
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(includedKey));
        assertEquals(includedValue, storedProperties.getProperty(includedKey));
    }
    
    @Test
    public void testExecuteIncludingPropertyKeysFromClasspathResource() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        mojo.setIncludePropertyKeysFromFiles(new String[] {"/eu/dnetlib/maven/plugin/properties/included.properties"});
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(includedKey));
        assertEquals(includedValue, storedProperties.getProperty(includedKey));
    }
    
    @Test(expected=MojoExecutionException.class)
    public void testExecuteIncludingPropertyKeysFromBlankLocation() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        mojo.setIncludePropertyKeysFromFiles(new String[] {""});
        
        // execute
        mojo.execute();
    }
    
    @Test
    public void testExecuteIncludingPropertyKeysFromXmlFile() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        File includedPropertiesFile = new File(testFolder.getRoot(), "included.xml");
        Properties includedProperties = new Properties();
        includedProperties.setProperty(includedKey, "irrelevantValue");
        includedProperties.storeToXML(new FileOutputStream(includedPropertiesFile), null);
        
        mojo.setIncludePropertyKeysFromFiles(new String[] {includedPropertiesFile.getAbsolutePath()});
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(1, storedProperties.size());
        assertTrue(storedProperties.containsKey(includedKey));
        assertEquals(includedValue, storedProperties.getProperty(includedKey));
    }
    
    @Test(expected=MojoExecutionException.class)
    public void testExecuteIncludingPropertyKeysFromInvalidXmlFile() throws Exception {
        // given
        String key = "projectPropertyKey";
        String value = "projectPropertyValue";
        String includedKey = "includedPropertyKey";
        String includedValue = "includedPropertyValue";
        Properties projectProperties = new Properties();
        projectProperties.setProperty(key, value);
        projectProperties.setProperty(includedKey, includedValue);
        doReturn(projectProperties).when(mavenProject).getProperties();
        
        File includedPropertiesFile = new File(testFolder.getRoot(), "included.xml");
        Properties includedProperties = new Properties();
        includedProperties.setProperty(includedKey, "irrelevantValue");
        includedProperties.store(new FileOutputStream(includedPropertiesFile), null);
        
        mojo.setIncludePropertyKeysFromFiles(new String[] {includedPropertiesFile.getAbsolutePath()});
        
        // execute
        mojo.execute();
    }
    
    @Test
    public void testExecuteWithQuietModeOn() throws Exception {
        // given
        mojo.setQuiet(true);
        mojo.setIncludePropertyKeysFromFiles(new String[] {"invalid location"});
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertEquals(0, storedProperties.size());
    }
    
    @Test(expected=MojoExecutionException.class)
    public void testExecuteIncludingPropertyKeysFromInvalidFile() throws Exception {
        // given
        mojo.setIncludePropertyKeysFromFiles(new String[] {"invalid location"});
        
        // execute
        mojo.execute();
    }
    
    @Test
    public void testExecuteWithEnvironmentProperties() throws Exception {
        // given
        mojo.setIncludeEnvironmentVariables(true);
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertTrue(storedProperties.size() > 0);
        for (Object currentKey : storedProperties.keySet()) {
            assertTrue(((String)currentKey).startsWith(PROPERTY_PREFIX_ENV));
        }
    }
    
    @Test
    public void testExecuteWithSystemProperties() throws Exception {
        // given
        String key = "systemPropertyKey";
        String value = "systemPropertyValue";
        System.setProperty(key, value);
        mojo.setIncludeSystemProperties(true);
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertTrue(storedProperties.size() > 0);
        assertTrue(storedProperties.containsKey(key));
        assertEquals(value, storedProperties.getProperty(key));
    }
    
    @Test
    public void testExecuteWithSystemPropertiesAndEscapeChars() throws Exception {
        // given
        String key = "systemPropertyKey ";
        String value = "systemPropertyValue";
        System.setProperty(key, value);
        mojo.setIncludeSystemProperties(true);
        String escapeChars = "cr,lf,tab,|";
        mojo.setEscapeChars(escapeChars);
        
        // execute
        mojo.execute();
        
        // assert
        assertTrue(mojo.outputFile.exists());
        Properties storedProperties = getStoredProperties();
        assertTrue(storedProperties.size() > 0);
        assertFalse(storedProperties.containsKey(key));
        assertTrue(storedProperties.containsKey(key.trim()));
        assertEquals(value, storedProperties.getProperty(key.trim()));
    }
    
    // ----------------------------------- PRIVATE -------------------------------------------
    
    private File getPropertiesFileLocation() {
        return new File(testFolder.getRoot(), "test.properties");
    }
    
    private Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(getPropertiesFileLocation()));
        return properties;
    }
}
