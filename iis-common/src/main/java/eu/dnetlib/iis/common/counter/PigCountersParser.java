package eu.dnetlib.iis.common.counter;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.counter.PigCounters.JobCounters;

/**
 * Parser of {@link PigCounters} from json string.
 * 
 * @author madryk
 */
public class PigCountersParser {

    private static final String JOB_IDS_COUNTER_NAME = "JOB_GRAPH";
    
    private static final String ALIASES_COUNTER_NAME = "Alias";
    
    private static final char VALUES_SEPARATOR = ',';
    
    
    private final JsonParser jsonParser = new JsonParser();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Parses input pig counters in json format into {@link PigCounters} object.
     */
    public PigCounters parse(String pigCountersJson) {
        
        JsonObject pigCountersJsonObject = jsonParser.parse(pigCountersJson).getAsJsonObject();
        
        List<String> jobIds = extractJobIds(pigCountersJsonObject);
        
        List<JobCounters> jobCountersList = jobIds.stream()
                .map(jobId -> extractJobCounters(pigCountersJsonObject, jobId))
                .collect(toList());
        
        Map<String, String> rootLevelCounters = extractRootLevelCounters(pigCountersJsonObject);
        
        return new PigCounters(rootLevelCounters, jobCountersList);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Extracts job ids from pig counters json.
     */
    private List<String> extractJobIds(JsonObject pigCountersJsonObject) {
        
        JsonElement element = pigCountersJsonObject.get(JOB_IDS_COUNTER_NAME);
        
        return Lists.newArrayList(StringUtils.split(element.getAsString(), VALUES_SEPARATOR));
        
    }
    
    /**
     * Extracts job counters from pig counters json.
     */
    private JobCounters extractJobCounters(JsonObject pigCountersJsonObject, String jobId) {
        
        JsonObject jobCountersJson = pigCountersJsonObject.get(jobId).getAsJsonObject();
        
        
        List<String> aliases = Arrays.asList(StringUtils.split(jobCountersJson.get(ALIASES_COUNTER_NAME).getAsString(), VALUES_SEPARATOR));
        
        Map<String, String> counters = buildCountersFromJson(jobCountersJson);
        
        
        return new JobCounters(jobId, aliases, counters);
    }
    
    /**
     * Extracts root level counters from pig counter json
     */
    private Map<String, String> extractRootLevelCounters(JsonObject pigCountersJsonObject) {
        
        return buildCountersFromJson(pigCountersJsonObject);

    }
    
    /**
     * Builds job counters map from json.<br/>
     * Method supports only primitive string values. Any counters
     * defined in json that doesn't follow this rule will
     * be omitted.<br/>
     * For example:<br/>
     * <code>buildCountersFromJson({"COUNTER_1": "aaa", "COUNTER_2": null, "COMPLEX_COUNTER": {"A": 1, "B": "bbb"})</code><br/>
     * will result in map with single element: <code>{"COUNTER_1": "aaa"}</code>
     */
    private Map<String, String> buildCountersFromJson(JsonObject jsonCounters) {
        
        Map<String, String> counters = Maps.newHashMap();
        
        for (Map.Entry<String, JsonElement> jsonCounter : jsonCounters.entrySet()) {
            JsonElement counterValue = jsonCounter.getValue();
            
            if (counterValue.isJsonPrimitive() && counterValue.getAsJsonPrimitive().isString()) {
                
                    counters.put(jsonCounter.getKey(), counterValue.getAsString());
            }
        }
        
        return counters;
    }
}
