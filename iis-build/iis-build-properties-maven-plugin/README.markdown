Maven plugin module utilized by `iis-workflows` for proper `job.properties` file building.

It is based on http://site.kuali.org/maven/plugins/properties-maven-plugin/1.3.2/write-project-properties-mojo.html and supplemented with:

* handling includePropertyKeysFromFiles property allowing writing only properties listed in given property files
As a final outcome only properties listed in `<include>` element and listed as a keys in files from `<includePropertyKeysFromFiles>` element will be written to output file. 
