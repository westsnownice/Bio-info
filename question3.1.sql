CREATE TEMPORARY TABLE IF NOT EXISTS common AS(
SELECT Bg.id 'Bgene_id', Bg.name 'Bgene_name', Bgih.id 'Bgih_id'
FROM Bgene as Bg 
INNER JOIN BgenInfoHuman as Bgih ON Bgih.symbol = Bg.`name`
WHERE Bg.`primary` = TRUE);

create temporary table if not exists BgenePrimaryName as(
select Bg.id, Bg.name, Bdg.disease
from Bgene as Bg inner join BdiseaseGene as Bdg on Bdg.gene = Bg.id
where Bg.primary = True);

create temporary table if not exists BgeneSynoName as(
select Bg.id, Bg.name, Bdg.disease
from Bgene as Bg inner join BdiseaseGene as Bdg on Bdg.gene = Bg.id
where Bg.synonym = TRUE
order by Bg.id asc);

create temporary table if not exists BgenePrimaryAndSynoName as (
select Bgpn.id 'primaryID', Bgpn.name 'primaryName', Bgsn.name 'secondName' 
from BgeneSynoName as Bgsn inner join BgenePrimaryName as Bgpn on Bgpn.disease = Bgsn.disease);


select DISTINCT temp.commonName from (
select Bgpasn.secondName 'secondNameUniProt', co.Bgene_name 'commonName', Bgis.name 'secondNameUCBI'
from common as co inner join BgeneInfoSyno as Bgis on Bgis.primaryName = co.Bgih_id
inner join BgenePrimaryAndSynoName as Bgpasn on Bgpasn.primaryID = co.Bgene_id
where Bgpasn.secondName != Bgis.name) as temp;

select count(distinct temp.commonName) from (
select Bgpasn.secondName 'secondNameUniProt', co.Bgene_name 'commonName', Bgis.name 'secondNameUCBI'
from common as co inner join BgeneInfoSyno as Bgis on Bgis.primaryName = co.Bgih_id
inner join BgenePrimaryAndSynoName as Bgpasn on Bgpasn.primaryID = co.Bgene_id
where Bgpasn.secondName != Bgis.name) as temp;
