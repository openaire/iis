package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;


/**
 * This is an utility class responsible for mapping country name to the ISO 3166-1 alpha-2 
 * country code by relying on a predefined set of mappings from {@link Locale} supplemented 
 * with auxiliary set of mappings explicitly defined in a property file.
 * 
 * @author mhorst
 */
public class CountryNameToCodeMapper {

    private static final String AUX_MAPPING_RESOURCE_LOCATION = "eu/dnetlib/iis/wf/ingest/pmc/metadata/auxiliary_country_name_to_code.properties";
    
    private Map<String, String> countryNameToCodeMap;

    // ------------------------ CONSTRUCTORS ---------------------------------
    
    /**
     * Default constructor preparing mappings.
     */
    public CountryNameToCodeMapper() {
        this(AUX_MAPPING_RESOURCE_LOCATION);
    }
    
    /**
     * Default constructor preparing mappings.
     * @param auxMappingResourceLocation auxiliary mapping classpath resource location
     */
    public CountryNameToCodeMapper(String auxMappingResourceLocation) {
        countryNameToCodeMap = new HashMap<>();
        initializeDefaultCountryCodes();
        try {
            loadAuxiliaryCountryCodesFromResourceFile(auxMappingResourceLocation);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // -------------------------- LOGIC ---------------------------------------
    
    /**
     * Provides country code for a given country name.
     * @param countryName country name
     * @return country code or null of not found
     */
    public String getCountryCode(String countryName) {
        return countryNameToCodeMap.get(normaizeCountryName(countryName));
    }
    
    // ------------------------------ PRIVATE ---------------------------------
    
    /**
     * Normalizes country name by uppercasing and trimming.
     * @return romalized country name.
     */
    private static String normaizeCountryName(String countryName) {
        if (countryName != null) {
            return countryName.trim().toUpperCase();
        } else {
            return null;
        }
    }
    
    /**
     * Initializes default set of country codes coming from {@link Locale}.
     */
    private void initializeDefaultCountryCodes() {
        for (String isoCode : Locale.getISOCountries()) {
            Locale l = new Locale(Locale.ENGLISH.getCountry(), isoCode);
            countryNameToCodeMap.put(normaizeCountryName(l.getDisplayCountry()), isoCode);
        }
        
    }

    /**
     * Loads auxiliary country name to country code mappings from classpath resource.
     * @param auxMappingResourceLocation country name to code mappings resource file classpath location
     * @throws IOException exception thrown when auxiliary mapping file could not be found
     */
    private void loadAuxiliaryCountryCodesFromResourceFile(String auxMappingResourceLocation) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(auxMappingResourceLocation)) {
            if (inputStream != null) {
                properties.load(inputStream);
                for (String key : properties.stringPropertyNames()) {
                    countryNameToCodeMap.put(normaizeCountryName(key), properties.getProperty(key).trim());
                }
            } else {
                throw new IOException("Properties file not found: " + auxMappingResourceLocation);
            }
        }
    }
 
}