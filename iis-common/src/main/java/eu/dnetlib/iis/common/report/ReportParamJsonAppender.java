package eu.dnetlib.iis.common.report;

import java.util.List;

import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import eu.dnetlib.iis.common.schemas.ReportParam;

/**
 * Appender of {@link ReportParam} into json report.
 * 
 * @author madryk
 */
public class ReportParamJsonAppender {


    //------------------------ LOGIC --------------------------
    
    /**
     * Appends {@link ReportParam} into the passed json report.<br/>
     * 
     * The key of the report param ({@link ReportParam#getKey()}) defines
     * where the value ({@link ReportParam#getValue()}) should be inserted
     * in report json structure.<br/>
     * The key is splitted by dots. Resulting values defines subsequent
     * fields in json.<br/>
     * For example:<br/>
     * <code>new ReportParam("param1.paramA", "34")</code><br/>
     * will result in following json:<br/>
     * <code>{"param1": {"paramA": 34}}</code><br/>
     * If report param key collides with passed json, then any
     * conflicting node will be replaced with new report param
     * value.
     */
    public void appendReportParam(JsonObject jsonReport, ReportParam reportParam) {
        
        String[] jsonFieldHierarchy = StringUtils.split(reportParam.getKey().toString(), '.');
        
        insertValue(jsonReport, Lists.newArrayList(jsonFieldHierarchy), convertParamValueToJson(reportParam));
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private JsonElement convertParamValueToJson(ReportParam reportParam) {
        
        long value = Long.valueOf(reportParam.getValue().toString());
        
        return new JsonPrimitive(value);
    }
    
    
    private void insertValue(JsonObject element, List<String> fieldHierarchy, JsonElement value) {
        
        String headField = fieldHierarchy.get(0);
        List<String> tailFieldHierarchy = fieldHierarchy.subList(1, fieldHierarchy.size());
        
        if (element.has(headField)) {
            JsonElement nextElement = element.get(headField);
            
            if (fieldHierarchy.size() == 1) {
                element.add(headField, value);
            } else if (!nextElement.isJsonObject()) {
                JsonElement elementWithValue = generateNode(tailFieldHierarchy, value);
                element.add(headField, elementWithValue);
            } else {
                insertValue(element.getAsJsonObject(headField), tailFieldHierarchy, value);
            }
        } else {
            JsonElement elementWithValue = generateNode(tailFieldHierarchy, value);
            element.add(headField, elementWithValue);
        }
        
    }
    
    private JsonElement generateNode(List<String> fieldHierarchy, JsonElement value) {
        JsonElement last = value;
        
        for (int i=fieldHierarchy.size()-1; i>=0; --i) {
            
            JsonObject parentJsonObject = new JsonObject();
            parentJsonObject.add(fieldHierarchy.get(i), last);
            
            last = parentJsonObject;
            
        }
        return last;
    }
}
