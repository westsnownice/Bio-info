CREATE TEMPORARY TABLE IF NOT EXISTS geneGoUniProt AS(
SELECT Bg.id, Bg.name, BGO.goID, BGO.term
FROM Bdisease as Bd
INNER JOIN BdiseaseGene as Bdg on Bdg.disease = Bd.id
INNER JOIN Bgene as Bg on Bg.id = Bdg.gene
INNER JOIN BdiseaseGo as Bdo on Bdo.disease = Bd.id
INNER JOIN BgenOntology as BGO on BGO.id = Bdo.go
order by Bg.id asc);


CREATE TEMPORARY TABLE IF NOT EXISTS geneGoUCBI AS(
SELECT Bgih.symbol, Bg2g.goID, Bg2g.goTerm
FROM BgenInfoHuman as Bgih 
INNER JOIN Bgene2go as Bg2g ON Bg2g.geneID = Bgih.geneID);


SELECT *
FROM geneGoUniProt as g1
INNER JOIN geneGoUCBI AS g2 ON g2.goID = g1.goID AND g2.symbol = g1.name
WHERE g1.term != g2.goTerm;


select * from geneGoUCBI
select * from geneGoUniProt

select count(*) from geneGoUCBI
select count(*) from geneGoUniProt

select *
from Bgene2go as Bg2g
where Bg2g.category = 'component'
limit 1000