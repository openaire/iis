/*====== countries: table of Country ISO codes , country names ===========*/
CREATE TABLE countries (
    country2 PRIMARY KEY UNIQUE,
    country_name 
);
insert into countries select country2, country_name from (file 'countries.tsv' delimiter:\t quoting:QUOTE_NONE header:t);
/*====== ilang Table Interface languages (name and portal lang code) ===========*/
CREATE TABLE ilang (
    lang_name TEXT,
    lang TEXT 
);
insert into ilang select lang_name , lang from (file 'ilang.tsv' delimiter:\t quoting:QUOTE_NONE header:t );
/*====== User_countries: table of id's of registered user's code ===========*/
CREATE TABLE user_countries (
    country  PRIMARY KEY,
    country_name 
);
