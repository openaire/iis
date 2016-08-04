package eu.dnetlib.iis.common.report;

import java.util.List;

import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Appender of {@link ReportEntry} into json report.
 * 
 * @author madryk
 */
public class ReportEntryJsonAppender {

    private ReportValueJsonConverterManager reportValueJsonConverterManager = new ReportValueJsonConverterManager();

    //------------------------ LOGIC --------------------------
    
    /**
     * Appends {@link ReportEntry} into the passed json report.<br/>
     * 
     * The key of the report param ({@link ReportEntry#getKey()}) defines
     * where the value ({@link ReportEntry#getValue()}) should be inserted
     * in report json structure.<br/>
     * The key is splitted by dots. Resulting values defines subsequent
     * fields in json.<br/>
     * For example:<br/>
     * <code>new ReportEntry("param1.paramA", ReportEntryType.COUNTER, "34")</code><br/>
     * will result in following json:<br/>
     * <code>{"param1": {"paramA": 34}}</code><br/>
     * If report entry key collides with passed json, then any
     * conflicting node will be replaced with new report entry
     * value.
     */
    public void appendReportEntry(JsonObject jsonReport, ReportEntry reportEntry) {
        
        String[] jsonFieldHierarchy = StringUtils.split(reportEntry.getKey().toString(), '.');
        
        insertValue(jsonReport, Lists.newArrayList(jsonFieldHierarchy), reportValueJsonConverterManager.convertValue(reportEntry));
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    /**
     * Inserts json value into the passed json object under location defined by fieldHierarchy list.<br/>
     * For example:<br/>
     * If <code>jsonObject = {"aaa": {"ccc":1}}</code>, <code>fieldHierarchy = ["aaa", "bbb"]</code> and <code>value = {"a": 5}</code>,
     * then jsonObject will be modified to: <code>{"aaa": {"ccc":1, "bbb": {"a": 5}}}</code>
     */
    private void insertValue(JsonObject jsonObject, List<String> fieldHierarchy, JsonElement value) {
        
        String headField = fieldHierarchy.get(0);
        List<String> tailFieldHierarchy = fieldHierarchy.subList(1, fieldHierarchy.size());
        
        if (jsonObject.has(headField)) {
            JsonElement nextElement = jsonObject.get(headField);
            
            if (fieldHierarchy.size() == 1) {
                jsonObject.add(headField, value);
            } else if (!nextElement.isJsonObject()) {
                JsonElement elementWithValue = generateNode(tailFieldHierarchy, value);
                jsonObject.add(headField, elementWithValue);
            } else {
                insertValue(jsonObject.getAsJsonObject(headField), tailFieldHierarchy, value);
            }
        } else {
            JsonElement elementWithValue = generateNode(tailFieldHierarchy, value);
            jsonObject.add(headField, elementWithValue);
        }
        
    }
    
    /**
     * Generates json element with inserted passed value under location 
     * defined by fieldHierarchy list.<br/>
     * For example:<br/>
     * If <code>fieldHierarchy = ["aaa", "bbb"]</code> and <code>value = {"a": 5}</code>, then
     * result of this method will be: <code>{"aaa": {"bbb": {"a": 5}}}</code>.
     */
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
