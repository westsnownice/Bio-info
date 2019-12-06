import xml.etree.cElementTree as ET
from db_connector import MySQLConnector

connect_mysql = MySQLConnector('127.0.0.1', 'root', database='bioinfo')
# connect_mysql = MySQLConnector('localhost', 'root', database='bioinfo')

'''
ID - name: entry name of disease (unique 1-1)
AC - accession: accession number of disease (1-N)
DE - protein name: corresponding protein name of the disease (1-N)
GN - gene name: corresponding gene name of the disease (1-N)
KW - keyword: keyword to describe the disease (1-N)
DR - gene ontology: annotation of corresponding gene ontology (1-N)
'''

ID = None

# NSMAP: take the XML namespace into consideration, otherwise findall() will return empty list
NSMAP = {'uni': 'http://uniprot.org/uniprot'}
path_data = 'data/congenital.xml'
tree = ET.parse(path_data)
root = tree.getroot()

i = 0
for entry in root.findall('.//uni:entry', NSMAP):
    i += 1
    print 'entry: %s' % i

    # id name
    for name in entry.findall('./uni:name', NSMAP):
        connect_mysql.insert('Bdisease', name=name.text)
        conditional_query = 'name= %s'
        ID = connect_mysql.select('Bdisease', conditional_query, 'id', name=name.text)

    # accession
    numAC = 0
    for accession in entry.findall('./uni:accession', NSMAP):
        numAC += 1
        AC = accession.text
        if 1 == numAC:
            connect_mysql.insert('Baccession', ac=AC, primary=True, disease=ID)
            conditional_query = 'id = %s'
            connect_mysql.update('Bdisease', conditional_query, ID, accession=AC)
        else:
            connect_mysql.insert('Baccession', ac=AC, primary=False, disease=ID)

    # protein name
    for protein in entry.findall('./uni:protein', NSMAP):
        for rec_name in protein.findall('./uni:recommendedName', NSMAP):
            '''---------------rec Name------------------------'''
            row = None
            for rec_full_name in rec_name.findall('./uni:fullName', NSMAP):
                DE_fullname = rec_full_name.text
                # check if the protein name already exist
                conditional_query = 'recFullName = %s'
                result = connect_mysql.select('Bprotein', conditional_query, 'id', recFullName=DE_fullname)

                # if not exits, insert to Bprotein, update Bdisease, add mapping in the BdiseaseProtein table
                if not result:
                    row = connect_mysql.insert('Bprotein', recFullName=DE_fullname)
                    conditional_query = 'id = %s'
                    connect_mysql.update('Bdisease', conditional_query, ID, protein=DE_fullname)
                    connect_mysql.insert('BdiseaseProtein', disease=ID, protein=row)
                # if exist, only update disease and add mapping
                else:
                    conditional_query = 'id = %s'
                    connect_mysql.update('Bdisease', conditional_query, ID, protein=DE_fullname)
                    connect_mysql.insert('BdiseaseProtein', disease=ID, protein=result)

            # update shortName adn Ec Number
            for rec_short_name in rec_name.findall('./uni:shortName', NSMAP):
                conditional_query = 'id = %s'
                connect_mysql.update('Bprotein', conditional_query, row, recShortName=rec_short_name.text)
            for rec_ec in rec_name.findall('./uni:ecNumber', NSMAP):
                conditional_query = 'id = %s'
                connect_mysql.update('Bprotein', conditional_query, row, recEcNumber=rec_ec.text)

    # gene name
    for gene in entry.findall('./uni:gene', NSMAP):
        for gene_name in gene.findall('./uni:name', NSMAP):
            gene_name_type = gene_name.get('type')
            # check if the gene already exist
            conditional_query = 'name = %s'
            result = connect_mysql.select('Bgene', conditional_query, 'id', 'primary', name=gene_name.text)

            # if not exist
            if not result:
                row = None
                if gene_name_type == 'primary':
                    row = connect_mysql.insert('Bgene', name=gene_name.text, primary=True, synonym=False,
                                               orderedLocus=False,
                                               orf=False)
                    conditional_query = 'id = %s'
                    connect_mysql.update('Bdisease', conditional_query, ID, geneName=gene_name.text)
                elif gene_name_type == 'synonym':
                    row = connect_mysql.insert('Bgene', name=gene_name.text, primary=False, synonym=True,
                                               orderedLocus=False, orf=False)
                elif gene_name_type == 'ordered locus':
                    row = connect_mysql.insert('Bgene', name=gene_name.text, primary=False, synonym=False,
                                               orderedLocus=True, orf=False)
                else:
                    row = connect_mysql.insert('Bgene', name=gene_name.text, primary=False, synonym=False,
                                               orderedLocus=False, orf=False)

                connect_mysql.insert('BdiseaseGene', disease=ID, gene=row)
            # if exist
            else:
                connect_mysql.insert('BdiseaseGene', disease=ID, gene=result[0][0])
                # if the name of gene is primary, put in the geneName column
                if result[0][1]:
                    conditional_query = 'id = %s'
                    connect_mysql.update('Bdisease', conditional_query, ID, geneName=gene_name.text)

    # Keyword
    for keyword in entry.findall('./uni:keyword', NSMAP):
        # check if the keyword is already included in the database
        conditional_query = 'description = %s'
        result = connect_mysql.select('Bkeyword', conditional_query, 'id', description=keyword.text)
        # if not exist
        if not result:
            row = connect_mysql.insert('Bkeyword', description=keyword.text)
            connect_mysql.insert('BdiseaseKeyword', disease=ID, keyword=row)
        else:
            connect_mysql.insert('BdiseaseKeyword', disease=ID, keyword=result)

    # gene ontology
    for db_ref in entry.findall('./uni:dbReference', NSMAP):
        db_type = db_ref.get('type')
        db_goId = None
        db_goValue = None
        if db_type == 'GO':
            db_goId = db_ref.get('id')
            for db_property in db_ref.findall('./uni:property', NSMAP):
                db_propertyType = db_property.get('type')
                if db_propertyType == 'term':
                    db_goValue = db_property.get('value')
        # check if the go has already included in the database
        if db_goId and db_goValue:
            db_goValue = db_goValue[2:]
            conditional_query = 'goID = %s'
            result = connect_mysql.select('BgenOntology', conditional_query, 'id', goID=db_goId)
            if not result:
                row = connect_mysql.insert('BgenOntology', goID=db_goId, term=db_goValue)
                connect_mysql.insert('BdiseaseGo', disease=ID, go=row)
            else:
                connect_mysql.insert('BdiseaseGo', disease=ID, go=result)

    print "entry %s finished" % i

connect_mysql.close()
