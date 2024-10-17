# About


All Scala, Java, and resource files in this module, located under the `pl/edu/icm/` subdirectories, were copied from the following locations in the [CoAnSys](https://github.com/CeON/CoAnSys/) repository:

https://github.com/CeON/CoAnSys/tree/master/citation-matching/citation-matching-core-code

https://github.com/CeON/CoAnSys/tree/master/ceon-scala-commons-lite

The sources were cloned from the [CoAnSys](https://github.com/CeON/CoAnSys/) repository at revision: `f0373f0416c93d3cff3a6ea137da09fba1481cad`.

The main reason for cloning those sources was recompiling the scala files with `2.11` compiler version, making java code `spark 2.4` compliant and removing the two dependencies `citation-matching-core-code` and `ceon-scala-commons-lite` to prevent classloader from loading incompatible (`scala 2.10` and `spark 1.6`) classes.

The code from the CoAnSys repository is licensed under the GNU Affero General Public License v3.0.
