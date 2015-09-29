Below you can find a list of people that contributed code to the IIS system (in order of appearance) before its move to GitHub (i.e., between September 2012 and September 2015) along with their:

- GitHub login,
- role in the project,
- area of the contribution related to IIS source code.

People:

- Mateusz Kobos ([mkobos](http://github.com/mkobos)) - team leader, developer
    - User and technical specification of IIS.
    - Implementation of some infrastructural elements at the beginning of the project.
    - Integration of the first [MadIS][]-based module (along with Dominika).
- Marek Horst ([marekhorst](http://github.com/marekhorst)) - lead developer
    - Integration of IIS with OpenAIRE's Information Space (importing data from various data sources and exporting data to Information Space's HBase).
    - Development of Maven plugin for automated deployment of IIS workflows.
    - Orchestrating IIS modules using Oozie workflow description and transformer nodes, writing new transformer nodes.
    - Development of persistent Hadoop cache for [CERMINE][] module. 
    - Development of functionality of streaming PDFs from Information Space's Object store.
    - Cooperating with admins on solving Hadoop problems, upgrading, and migrating Hadoop.
    - Making the code compatible with subsequent versions of Hadoop (CDH3, CDH4, CDH5).
    - Maintenance of project's build system (Maven projects) - making sure that it works correctly, updating it if necessary.
    - Maintenance and reorganization of project's code base - removing things that are not needed any more etc. This includes extension and reorganization of modules written by other developers (e.g. EuropePMC citation ingestion and parsing).
    - Monitoring and modifying Jenkins integration tests.
    - Testing performance of individual modules and reporting related problems to solve; debugging the modules.
    - Using IIS'es "generic collapsers" solution to merge data coming from various sources.
    - Integration of [MadIS]-based modules with IIS.
    - Integration of [CERMINE][]-based functionality of extracting citation sentiment from papers.
- Dominika Tkaczyk ([dtkaczyk](http://github.com/dtkaczyk)) - developer
    - Development of Pig transformers that transform data produced by one module to the form expected by another module.
    - Integration of [MadIS]-based modules with IIS and preparing base infrastructure for subsequent integrations of these modules.
    - Development of statistics module based on Hive technology.
    - Development of importer of plaintexts from EuropePMC.
    - Adjusting CERMINE to OpenAIRE requirements.
    - Development of "generic collapsers" for merging data coming from various sources.
- Mateusz Fedoryszak ([matfed](http://github.com/matfed)) - developer
    - Integration of [CoAnSys][]'s citation matching and document similarity modules with IIS.
    - Bugfixing and rewriting the citation matching module in order to make it work on large amounts of data in OpenAIRE.
    - Modification of the citation matching algorithm so it can handle the kind of incomplete metadata records that are stored in Information Space.
    - Implementation of ingestion of citation entries coming from EuropePMC XMLs.
- Michał Oniszczuk ([miconi](http://github.com/miconi)) - developer
    - Development of Pig transformers and "project references merger" functionality.
    - Application of the "generic collapser" infrastructural modules to implement merging workflows for data coming from various sources.
    - Exploration of possibilities of removing boilerplate code from IIS workflows.
- Piotr Dendek ([pdendek](http://github.com/pdendek)) - developer
    - Modification of [CoAnSys][]'s document similarity module to match OpenAIRE requirements.

[CoAnSys]: https://github.com/CeON/CoAnSys
[MadIS]: https://code.google.com/p/madis/
[CERMINE]: https://github.com/CeON/CERMINE
