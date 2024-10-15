# About

All the scala, java and resource files in this module stored within the pl/edu/icm/ subdirectories were copied from the following CoAnSys repository locations:

https://github.com/CeON/CoAnSys/tree/master/citation-matching/citation-matching-coansys-code

https://github.com/CeON/CoAnSys/tree/master/ceon-scala-commons-lite

The main reason for cloning those sources was recompiling the scala files with 2.11 compiler version, making java code spark2.4 compliant and removing the two dependencies citation-matching-coansys-code and ceon-scala-commons-lite to prevent classloader from loading incompatible (scala 2.10 and spark1.6) classes.

The code from the CoAnSys repository is licensed under the GNU Affero General Public License v3.0.
