package eu.dnetlib.iis.wf.importer.stream.project;

import com.google.gson.Gson;

import java.util.List;

public class ProjectDetail {
    private String projectId;
    private String acronym;
    private String code;
    private String optional1;
    private String optional2;
    private String jsonextrainfo;
    private List<String> fundingPath;

    public static ProjectDetail fromJson(final String json) {
        return new Gson().fromJson(json, ProjectDetail.class);
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getOptional1() {
        return optional1;
    }

    public void setOptional1(String optional1) {
        this.optional1 = optional1;
    }

    public String getOptional2() {
        return optional2;
    }

    public void setOptional2(String optional2) {
        this.optional2 = optional2;
    }

    public String getJsonextrainfo() {
        return jsonextrainfo;
    }

    public void setJsonextrainfo(String jsonextrainfo) {
        this.jsonextrainfo = jsonextrainfo;
    }

    public List<String> getFundingPath() {
        return fundingPath;
    }

    public void setFundingPath(List<String> fundingPath) {
        this.fundingPath = fundingPath;
    }
}
