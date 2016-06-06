package eu.dnetlib.iis.wf.export.actionmanager.module;

/**
 * List of algorithm names. 
 * 
 * Those names are also used as action set identifier and trust level threshold property keys suffixes.
 * 
 * @author mhorst
 *
 */
public enum AlgorithmName {
	
	document_similarities_standard,
	document_affiliations,
	document_classes,
	document_referencedProjects,
	document_referencedDatasets,
	document_referencedDocuments,
	document_research_initiative,
	document_pdb,
	document_software_url

}
