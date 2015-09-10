Maven plugin module utilized by `icm-iis-parent-container` for proper `job.properties` file building.
Based on:
http://site.kuali.org/maven/plugins/properties-maven-plugin/1.3.2/write-project-properties-mojo.html
Supplemented with:
* handling includePropertyKeysFromFiles property allowing writing only properties listed in given property files
As a final outcome only properties listed in <include> element and listed as a keys in files from <includePropertyKeysFromFiles> element will be written to output file. 
