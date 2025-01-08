package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * This is an utility class responsible for mapping country name to the ISO 3166-1 alpha-2 
 * country code by relying on a predefined set of mappings from {@link Locale} supplemented 
 * with auxiliary set of mappings explicitly defined in a JSON file.
 * 
 * @author mhorst
 */
public class CountryNameToCodeMapper {

    private static final String AUX_MAPPING_FILE_LOCATION = "auxiliary_country_name_to_code.json";
    
    private Map<String, String> countryNameToCodeMap;

    // ------------------------ CONSTRUCTORS ---------------------------------
    
    /**
     * Default constructor preparing mappings.
     */
    public CountryNameToCodeMapper() {
        this(AUX_MAPPING_FILE_LOCATION);
    }
    
    /**
     * Default constructor preparing mappings.
     * @param auxMappingResourceLocation auxiliary mapping classpath resource location
     */
    public CountryNameToCodeMapper(String auxMappingResourceLocation) {
        countryNameToCodeMap = new HashMap<>();
        initializeDefaultCountryCodes();
        try {
            loadAuxiliaryCountryCodesFromFile(auxMappingResourceLocation);
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
            Locale l = new Locale(Locale.ENGLISH.getLanguage(), isoCode);
            countryNameToCodeMap.put(normaizeCountryName(l.getDisplayCountry(Locale.ENGLISH)), isoCode);
        }
    }

    /**
     * Loads auxiliary country name to country code mappings from classpath resource.
     * @param auxMappingJsonFileLocation country name to code mappings json file classpath location
     * @throws IOException exception thrown when auxiliary mapping file could not be found
     */
    private void loadAuxiliaryCountryCodesFromFile(String auxMappingJsonFileLocation) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (InputStream inputStream = getClass().getResourceAsStream(auxMappingJsonFileLocation)) {
            if (inputStream != null) {
                Map<String,String> auxMappings = objectMapper.readValue(inputStream, new TypeReference<Map<String, String>>() {});
                for (Entry<String, String> entry :auxMappings.entrySet()) {
                    countryNameToCodeMap.put(normaizeCountryName(entry.getKey()), entry.getValue().trim());
                }
            } else {
                throw new IOException("JSON file with country name to country code mappings not found: " + auxMappingJsonFileLocation);
            }
        }
    }
 
}