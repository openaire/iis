package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Constants used in alternative organization names module
 * 
 * @author madryk
 */
public final class OrganizationAltNameConst {

    private static final String BASE_CLASSPATH = "/eu/dnetlib/iis/wf/affmatching/universities";
    
    
    /**
     * Default list of classpath csv files with alternative organization names
     */
    public static final List<String> CLASSPATH_ALTERNATIVE_NAMES_CSV_FILES = ImmutableList.of(
            BASE_CLASSPATH + "/universities_at.csv",
            BASE_CLASSPATH + "/universities_be.csv",
            BASE_CLASSPATH + "/universities_bg.csv",
            BASE_CLASSPATH + "/universities_cy.csv",
            BASE_CLASSPATH + "/universities_cz.csv",
            BASE_CLASSPATH + "/universities_de.csv",
            BASE_CLASSPATH + "/universities_dk.csv",
            BASE_CLASSPATH + "/universities_ee.csv",
            BASE_CLASSPATH + "/universities_es.csv",
            BASE_CLASSPATH + "/universities_fi.csv",
            BASE_CLASSPATH + "/universities_fr.csv",
            BASE_CLASSPATH + "/universities_gr.csv",
            BASE_CLASSPATH + "/universities_hr.csv",
            BASE_CLASSPATH + "/universities_hu.csv",
            BASE_CLASSPATH + "/universities_it.csv",
            BASE_CLASSPATH + "/universities_lt.csv",
            BASE_CLASSPATH + "/universities_lu.csv",
            BASE_CLASSPATH + "/universities_lv.csv",
            BASE_CLASSPATH + "/universities_nl.csv",
            BASE_CLASSPATH + "/universities_pl.csv",
            BASE_CLASSPATH + "/universities_pt.csv",
            BASE_CLASSPATH + "/universities_ro.csv",
            BASE_CLASSPATH + "/universities_se.csv",
            BASE_CLASSPATH + "/universities_si.csv",
            BASE_CLASSPATH + "/universities_sk.csv"
            );
    
    //------------------------ CONSTRUCTORS --------------------------
    
    private OrganizationAltNameConst() { }
    
    
}
