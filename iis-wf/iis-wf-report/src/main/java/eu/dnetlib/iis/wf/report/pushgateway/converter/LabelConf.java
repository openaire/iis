package eu.dnetlib.iis.wf.report.pushgateway.converter;

import java.util.Objects;

/**
 * Configuration of a label to be used for gauge creation. Label configuration is given by label name and a pattern.
 * Pattern is used to extract label value from report entry's key. It consists of a dollar sign an a number indicating
 * the position of report entry's key part to be used as label value.
 */
public class LabelConf {
    private String labelName;
    private String pattern;

    public LabelConf() {
    }

    public LabelConf(String labelName, String pattern) {
        this.labelName = labelName;
        this.pattern = pattern;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LabelConf labelConf = (LabelConf) o;
        return Objects.equals(labelName, labelConf.labelName) &&
                Objects.equals(pattern, labelConf.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelName, pattern);
    }
}
