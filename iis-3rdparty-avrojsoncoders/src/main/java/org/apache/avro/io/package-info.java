/**
 * Provides hacked version of classes used to convert between Avro and JSON
 * formats. The hacked version allows for not defining explicitly types of
 * fields in a union when one of the union-ed fields is a null.
 * 
 * Generally the problem with the original implementation is in the way 
 * the Avro library converts JSON to Avro file when you use the schema with 
 * a "union" field (and we use this kind of field a lot to make a certain 
 * field null-able (e.g. "union {null, int} age")). 
 * 
 * For example when you use the schema (Avro IDL format):
 * 
 * record Document{
 *   int id;
 *   union {null, string} title=null;
 *   array<int> authorIds;
 * }
 * 
 * The JSON that Avro expects must have the type of the union field 
 * defined explicitly:
 * 
 * {"authorIds": [1, 20], "id": 20, "title": {"string": "An extraordinary book"}}
 * {"authorIds": [10, 6, 1], "id": 1, "title": {"string": "Basics of the basics"}}
 * {"authorIds": [], "id": 2, "title": null}
 * {"authorIds": [1, 6], "id": 6, "title": {"string": "Even more of interesting stuff"}}
 *
 * One way of dealing with it is just enforcing production of JSON conforming 
 * to this convention by our modules. However such an approach seems to 
 * introduce an unneeded dose of complexity to the produced JSON, 
 * so I decided to modify the Avro library to make it accept JSON without the
 * type in the union field defined explicitly. 
 *
 * This convention of defining JSON structure is necessary in the general case 
 * (see http://mail-archives.apache.org/mod_mbox/avro-user/201206.mbox/%3CCALEq1Z84=20PfJahT7DKtXEAGPFfOoHtVYXiyi+O+cZYGdXBNQ@mail.gmail.com%3E), 
 * but in our case of using "union" only to mark a field nullable 
 * it does not seem to be needed.
 */
package org.apache.avro.io;