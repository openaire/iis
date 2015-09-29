# About

Information Inference Service (IIS) a flexible data processing system for handling big data based on Apache Hadoop technologies. It is a subsystem of the OpenAIRE system ([www.openaire.eu](http://www.openaire.eu) is its public web front-end) - see Fig.1 for a high-level overview.

![](https://raw.githubusercontent.com/openaire/iis/master/docs/system_architecture_pics/oa_architecture.png "An example of dimensionality reduction and outliers detection in PCA")

**Fig.1**: The center of OpenAIRE system is the Information Space system which stores all information available in the system. IIS ingests data from Information Space, runs processing workflows, and produces inferred data which, in turn, is ingested by Information Space.

The goal of OpenAIRE is to provide an infrastructure for gathering, processing (including de-duplication), and providing unified access to research-related data (papers, datasets, researchers, projects, etc.). The goal of IIS is to provide data/text mining functionality for the OpenAIRE system. In practice, IIS defines data processing workflows that connect various modules, each one with well-defined input and output. A high-level overview of IIS can be found in paper ["Information Inference in Scholarly Communication Infrastructures: The OpenAIREplus Project Experience", Procedia Computer Science, vol. 38, 2014, 92-99](http://www.sciencedirect.com/science/article/pii/S1877050914013763).

IIS was initially developed during [OpenAIREplus](http://cordis.europa.eu/project/rcn/100079_en.html) project and has been further extended during [OpenAIRE2020](http://cordis.europa.eu/project/rcn/194062_en.html) project.

The original code was migrated to GitHub from [D-NET](http://www.d-net.research-infrastructures.eu/) SVN repository. The public read-only interface of the repository is available at [https://svn-public.driver.research-infrastructures.eu/driver/dnet40/modules/](https://svn-public.driver.research-infrastructures.eu/driver/dnet40/modules/) and this is where you can find the history of the code base before the migration (IIS-related Maven projects are the ones matching glob pattern `*-iis-*`).

# Content of the most important subdirectories and files

- `docs` - basic documentation
- `iis-core` - generic common utilities used by other projects
- `iis-common` - OpenAIRE-related common utilities
- `iis-workflows` - definitions of workflows used in the system
- `CONTRIBUTORS.markdown` - list of contributors to the project
