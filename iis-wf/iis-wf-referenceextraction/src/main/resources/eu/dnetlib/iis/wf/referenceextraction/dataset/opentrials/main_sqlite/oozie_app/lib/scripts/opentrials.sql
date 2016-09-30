select distinct jdict('documentId', pid, 'datasetId', id, 'confidenceLevel', 0.8) from (
select * from (
   select pid,id,prev,middle,next, 
	  case when regexprmatches("^[^a-zA-Z]+$",middle) 
		 or length(middle)<7 
		 then regexprmatches("\btrial\b|\btrials\b|opentrial|clinicaltrial",lower(prev||next)) 
		 else 2 
		 end as confidence from (
	  select pid,acronym,id,prev,stripchars(middle,'()--') as middle,next from 
	 	(setschema 'pid,prev,middle,next' 
		select id as pid, textwindow2s(comprspaces(regexpr("\n",text," ")),10,1,5,"(BR\/\w+)|(C\d{5,})|(\d{6,})|NTR\d+|\w{10,}|^(?=.*[\w])(?=.*\d)(?=.*[\W_])[\w\d\W_]{7,}|\d+\/\d+") from 
		(select case when id is null then "" else id end as id,case when text is null then "" else text end as text from (setschema 'id,text' select jsonpath(c1, '$.id', '$.text') from stdinput()))
		),
		opentrials
			where stripchars(middle,'()--') = acronym))
				where confidence>0);
;
