package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Constants used in alternative organization names module
 * 
 * @author madryk
 */
public interface OrganizationAltNameConst {

    /**
     * Default list of classpath csv files with alternative organization names
     */
    List<String> CLASSPATH_ALTERNATIVE_NAMES_CSV_FILES = ImmutableList.of(
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_at.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_be.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_bg.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_cy.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_cz.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_de.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_dk.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_ee.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_es.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_fi.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_fr.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_gr.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_hr.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_hu.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_it.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_lt.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_lu.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_lv.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_nl.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_pl.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_pt.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_ro.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_se.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_si.csv",
            "/eu/dnetlib/iis/wf/affmatching/universities/universities_sk.csv"
            );
}
