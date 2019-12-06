import pandas as pd
from db_connector import MySQLConnector

connect_mysql = MySQLConnector('127.0.0.1', 'root', database='bioinfo')

df_gene2go = pd.read_table('data/gene2go')
df_geneInfo = pd.read_table('data/Homo_sapiens.gene_info')

'''
for index, row in df_gene2go.iterrows():
    connect_mysql.insert('Bgene2go', taxID=row['#tax_id'], geneID=row['GeneID'], goID=row['GO_ID'],
                         evidence=row['Evidence'], qualifier=row['Qualifier'], goTerm=row['GO_term'],
                         pubMed=row['PubMed'], category=row['Category'])
'''
for index, row in df_geneInfo.iterrows():
    rowid = connect_mysql.insert('BgenInfoHuman', taxID=row['#tax_id'], geneID=row['GeneID'], symbol=row['Symbol'],
                                 locusTag=row['LocusTag'], dfXrefs=row['dbXrefs'],
                                 chromosome=row['chromosome'], mapLocation=row['map_location'],
                                 description=row['description'], typeGene=row['type_of_gene'],
                                 symbolNA=row['Symbol_from_nomenclature_authority'],
                                 fullNameNA=row['Full_name_from_nomenclature_authority'],
                                 nomenclatureStatus=row['Nomenclature_status'],
                                 otherDesignation=row['Other_designations'],
                                 modifData=row['Modification_date'])
    synonyms = row['Synonyms'].split('|')
    for syno in synonyms:
        connect_mysql.insert('BgeneInfoSyno', name=syno, primaryName=rowid)

connect_mysql.close()
