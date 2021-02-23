drop table if exists arrayexpress_experiments;
create table arrayexpress_experiments as
select * from file('https://www.ebi.ac.uk/arrayexpress/ArrayExpress-Experiments.txt', 'delimiter:\t',  'header:t', 'encoding:ascii');

