A = load '$person' using PigStorage(',');
B = foreach A generate $0 as id;
store B into '$person_id' USING PigStorage();
