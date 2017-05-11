/**
 * 
 * Licensed under the Educational Community License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/ecl2.php
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.dnetlib.maven.plugin.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Writes project properties for the keys listed in specified properties files.
 * Based on: 
 * http://site.kuali.org/maven/plugins/properties-maven-plugin/1.3.2/write-project-properties-mojo.html

 * @author mhorst
 * @goal write-project-properties
 */
public class WritePredefinedProjectProperties extends AbstractMojo {
	
	private static final String CR = "\r";
	private static final String LF = "\n";
	private static final String TAB = "\t";
    protected static final String PROPERTY_PREFIX_ENV = "env.";
    private static final String ENCODING_UTF8 = "utf8";
	
	/**
	 * @parameter property="properties.includePropertyKeysFromFiles"
	 */
	private String[] includePropertyKeysFromFiles;
	
    /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;

    /**
     * The file that properties will be written to
     * 
     * @parameter property="properties.outputFile"
     *            default-value="${project.build.directory}/properties/project.properties";
     * @required
     */
    protected File outputFile;
	
	/**
     * If true, the plugin will silently ignore any non-existent properties files, and the build will continue
     *
     * @parameter property="properties.quiet" default-value="true"
     */
    private boolean quiet;
	
    /**
     * Comma separated list of characters to escape when writing property values. cr=carriage return, lf=linefeed,
     * tab=tab. Any other values are taken literally.
     * 
     * @parameter default-value="cr,lf,tab" property="properties.escapeChars"
     */
    private String escapeChars;

    /**
     * If true, the plugin will include system properties when writing the properties file. System properties override
     * both environment variables and project properties.
     * 
     * @parameter default-value="false" property="properties.includeSystemProperties"
     */
    private boolean includeSystemProperties;

    /**
     * If true, the plugin will include environment variables when writing the properties file. Environment variables
     * are prefixed with "env". Environment variables override project properties.
     * 
     * @parameter default-value="false" property="properties.includeEnvironmentVariables"
     */
    private boolean includeEnvironmentVariables;

    /**
     * Comma separated set of properties to exclude when writing the properties file
     * 
     * @parameter property="properties.exclude"
     */
    private String exclude;

    /**
     * Comma separated set of properties to write to the properties file. If provided, only the properties matching
     * those supplied here will be written to the properties file.
     * 
     * @parameter property="properties.include"
     */
    private String include;

    /* (non-Javadoc)
     * @see org.apache.maven.plugin.AbstractMojo#execute()
     */
    @Override
    @SuppressFBWarnings({"NP_UNWRITTEN_FIELD","UWF_UNWRITTEN_FIELD"})
    public void execute() throws MojoExecutionException, MojoFailureException {
        Properties properties = new Properties();
        // Add project properties
        properties.putAll(project.getProperties());
        if (includeEnvironmentVariables) {
            // Add environment variables, overriding any existing properties with the same key
            properties.putAll(getEnvironmentVariables());
        }
        if (includeSystemProperties) {
            // Add system properties, overriding any existing properties with the same key
            properties.putAll(System.getProperties());
        }

        // Remove properties as appropriate
        trim(properties, exclude, include);

        String comment = "# " + new Date() + "\n";
        List<String> escapeTokens = getEscapeChars(escapeChars);

        getLog().info("Creating " + outputFile);
        writeProperties(outputFile, comment, properties, escapeTokens);
	    }

    /**
     * Provides environment variables.
     * @return environment variables
     */
    protected static Properties getEnvironmentVariables() {
        Properties props = new Properties();
        for (Entry<String, String> entry : System.getenv().entrySet()) {
            props.setProperty(PROPERTY_PREFIX_ENV + entry.getKey(), entry.getValue());
        }
        return props;
    }

    /**
     * Removes properties which should not be written.
     * @param properties
     * @param omitCSV
     * @param includeCSV
     * @throws MojoExecutionException
     */
    protected void trim(Properties properties, String omitCSV, String includeCSV) throws MojoExecutionException {
        List<String> omitKeys = getListFromCSV(omitCSV);
        for (String key : omitKeys) {
            properties.remove(key);
        }
        
        List<String> includeKeys = getListFromCSV(includeCSV);
//      mh: including keys from predefined properties
        if (includePropertyKeysFromFiles!=null && includePropertyKeysFromFiles.length>0) {
        	for (String currentIncludeLoc : includePropertyKeysFromFiles) {
        		if (validate(currentIncludeLoc)) {
        			Properties p = getProperties(currentIncludeLoc);
        			for (String key : p.stringPropertyNames()) {
        				includeKeys.add(key);
        			}
        		}
        	}
        }
        if (includeKeys!=null && !includeKeys.isEmpty()) {
//        	removing only when include keys provided
        	Set<String> keys = properties.stringPropertyNames();
            for (String key : keys) {
                if (!includeKeys.contains(key)) {
                    properties.remove(key);
                }
            }	
        }
    }

    /**
     * Checks whether file exists.
     * @param location
     * @return true when exists, false otherwise.
     */
    protected boolean exists(String location) {
        if (StringUtils.isBlank(location)) {
            return false;
        }
        File file = new File(location);
        if (file.exists()) {
            return true;
        }
        ResourceLoader loader = new DefaultResourceLoader();
        Resource resource = loader.getResource(location);
        return resource.exists();
    }

    /**
     * Validates resource location.
     * @param location
     * @return true when valid, false otherwise
     * @throws MojoExecutionException
     */
    protected boolean validate(String location) throws MojoExecutionException {
        boolean exists = exists(location);
        if (exists) {
            return true;
        }
        if (quiet) {
            getLog().info("Ignoring non-existent properties file '" + location + "'");
            return false;
        } else {
            throw new MojoExecutionException("Non-existent properties file '" + location + "'");
        }
    }

    /**
     * Provides input stream.
     * @param location
     * @return input stream
     * @throws IOException
     */
    protected InputStream getInputStream(String location) throws IOException {
        File file = new File(location);
        if (file.exists()) {
            return new FileInputStream(location);
        }
        ResourceLoader loader = new DefaultResourceLoader();
        Resource resource = loader.getResource(location);
        return resource.getInputStream();
    }

    /**
     * Creates properties for given location.
     * @param location
     * @return properties for given location
     * @throws MojoExecutionException
     */
    protected Properties getProperties(String location) throws MojoExecutionException {
        InputStream in = null;
        try {
            Properties properties = new Properties();
            in = getInputStream(location);
            if (location.toLowerCase().endsWith(".xml")) {
                properties.loadFromXML(in);
            } else {
                properties.load(in);
            }
            return properties;
        } catch (IOException e) {
            throw new MojoExecutionException("Error reading properties file " + location, e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * Provides escape characters.
     * @param escapeChars
     * @return escape characters
     */
    protected List<String> getEscapeChars(String escapeChars) {
        List<String> tokens = getListFromCSV(escapeChars);
        List<String> realTokens = new ArrayList<String>();
        for (String token : tokens) {
            String realToken = getRealToken(token);
            realTokens.add(realToken);
        }
        return realTokens;
    }

    /**
     * Provides real token.
     * @param token
     * @return real token
     */
    protected String getRealToken(String token) {
        if (token.equalsIgnoreCase("CR")) {
            return CR;
        } else if (token.equalsIgnoreCase("LF")) {
            return LF;
        } else if (token.equalsIgnoreCase("TAB")) {
            return TAB;
        } else {
            return token;
        }
    }

    /**
     * Returns content.
     * @param comment
     * @param properties
     * @param escapeTokens
     * @return content
     */
    protected String getContent(String comment, Properties properties, List<String> escapeTokens) {
        List<String> names = new ArrayList<String>(properties.stringPropertyNames());
        Collections.sort(names);
        StringBuilder sb = new StringBuilder();
        if (!StringUtils.isBlank(comment)) {
            sb.append(comment);
        }
        for (String name : names) {
            String value = properties.getProperty(name);
            String escapedValue = escape(value, escapeTokens);
            sb.append(name + "=" + escapedValue + "\n");
        }
        return sb.toString();
    }

    /**
     * Writes properties to given file.
     * @param file
     * @param comment
     * @param properties
     * @param escapeTokens
     * @throws MojoExecutionException
     */
    protected void writeProperties(File file, String comment, Properties properties, List<String> escapeTokens)
            throws MojoExecutionException {
        try {
            String content = getContent(comment, properties, escapeTokens);
            FileUtils.writeStringToFile(file, content, ENCODING_UTF8);
        } catch (IOException e) {
            throw new MojoExecutionException("Error creating properties file", e);
        }
    }
	 
    /**
     * Escapes characters.
     * @param s
     * @param escapeChars
     * @return
     */
    protected String escape(String s, List<String> escapeChars) {
    	String result = s;
        for (String escapeChar : escapeChars) {
            result = result.replace(escapeChar, getReplacementToken(escapeChar));
        }
        return result;
    }

    /**
     * Provides replacement token.
     * @param escapeChar
     * @return replacement token
     */
    protected String getReplacementToken(String escapeChar) {
        if (escapeChar.equals(CR)) {
            return "\\r";
        } else if (escapeChar.equals(LF)) {
            return "\\n";
        } else if (escapeChar.equals(TAB)) {
            return "\\t";
        } else {
            return "\\" + escapeChar;
        }
    }

    /**
	 * Returns list from csv.
	 * @param csv
	 * @return list of values generated from CSV
	 */
	protected static final List<String> getListFromCSV(String csv) {
		if (StringUtils.isBlank(csv)) {
			return new ArrayList<String>();
		}
		List<String> list = new ArrayList<String>();
		String[] tokens = StringUtils.split(csv, ",");
		for (String token : tokens) {
			list.add(token.trim());
		}
		return list;
	}

    public void setIncludeSystemProperties(boolean includeSystemProperties) {
        this.includeSystemProperties = includeSystemProperties;
    }

    public void setEscapeChars(String escapeChars) {
        this.escapeChars = escapeChars;
    }

    public void setIncludeEnvironmentVariables(boolean includeEnvironmentVariables) {
        this.includeEnvironmentVariables = includeEnvironmentVariables;
    }

    public void setExclude(String exclude) {
        this.exclude = exclude;
    }

    public void setInclude(String include) {
        this.include = include;
    }

    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }
	
	/**
	 * Sets property files for which keys properties should be included.
	 * @param includePropertyKeysFromFiles
	 */
	public void setIncludePropertyKeysFromFiles(
			String[] includePropertyKeysFromFiles) {
		if (includePropertyKeysFromFiles!=null) {
			this.includePropertyKeysFromFiles = Arrays.copyOf(
					includePropertyKeysFromFiles, 
					includePropertyKeysFromFiles.length);	
		}
	}
	
}