CREATE DATABASE IF NOT EXISTS ${DB_NAME};

DROP TABLE IF EXISTS ${DB_NAME}.${TABLE_NAME};

CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id string, location string, mimetype string, size string, hash string) STORED AS PARQUET;

INSERT INTO TABLE ${DB_NAME}.${TABLE_NAME} VALUES 
    ('doaj10829873::0092cca2055a02f51c0b21b332395b09', 'http://localhost:8280/is/mvc/objectStore/retrieve.do?objectStore=doaj10829873%3A%3A0092cca2055a02f51c0b21b332395b09%3A%3A2ed5ca4d0d8f77aaf2ef4fbf12418750&objectId=doaj10829873%3A%3A0092cca2055a02f51c0b21b332395b09%3A%3A2ed5ca4d0d8f77aaf2ef4fbf12418750', 'text/html', '38912', '19a53e9ebb0c878ad088568a8cf23dc6'), 
    ('od______2367::0001a50c6388e9bfcb791a924ec4b837', 'http://localhost:8280/is/mvc/objectStore/retrieve.do?objectStore=od______2367%3A%3A0001a50c6388e9bfcb791a924ec4b837%3A%3A80aedf56c7fedf81758414e3b5af1f70&objectId=od______2367%3A%3A0001a50c6388e9bfcb791a924ec4b837%3A%3A80aedf56c7fedf81758414e3b5af1f70', 'application/pdf', '1024', '3a0fb023de6c8735a52ac9c1edace612'), 
    ('dedup1______::000015093c397516c0b1b000f38982de', 'http://localhost:8280/is/mvc/objectStore/retrieve.do?objectStore=webcrawl____%3A%3A000015093c397516c0b1b000f38982de&objectId=webcrawl____%3A%3A000015093c397516c0b1b000f38982de', 'file::WoS', '6144', 'a9af919ba339a1491a922a7fe7b2580f'), 
    ('od_______908::00005ab6b308ff8f0f5415be03d8cce9', 'http://localhost:8280/is/mvc/objectStore/retrieve.do?objectStore=od_______908%3A%3A00005ab6b308ff8f0f5415be03d8cce9&objectId=od_______908%3A%3A00005ab6b308ff8f0f5415be03d8cce9', 'xml', '7168', '464ac8be9b33e109e98596799cf9d05d');