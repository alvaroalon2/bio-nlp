import pandas as pd 
import numpy as np


from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


important_columns = {
    'cid',
    'cmpdname',
    'cmpdsynonym',
    'inchikey',
    'meshheadings'
}


pubchem_df = pd.read_csv(
    "data/PubChem.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns,
    dtype=str
)

print(pubchem_df.shape)
pubchem_df.head()


pubchem_df.fillna('', inplace=True)


pubchem_df.sample(5)


important_columns_chebi = {
    'Class ID',
    'Preferred Label',
    'Synonyms',
    'Obsolete',
    'Parents',
    'database_cross_reference',
    'http://purl.obolibrary.org/obo/chebi/inchikey',
    'http://www.w3.org/2004/02/skos/core#notation'
}


chebi_df = pd.read_csv(
    "data/CHEBI.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_chebi,
    dtype=str
)

print(chebi_df.shape)
chebi_df.head()


chebi_df = chebi_df[chebi_df['Obsolete']=='false']
chebi_df.drop(['Obsolete','Class ID'],axis = 1, inplace=True)
print(chebi_df.shape)
chebi_df.head()


renamed_columns_chebi = {
    'Preferred Label':'ChEBI_label',
    'Synonyms': 'ChEBI_synonyms',
    'Parents':'ChEBI_parents',
    'database_cross_reference':'ChEBI_cross_reference',
    'http://purl.obolibrary.org/obo/chebi/inchikey':'inchikey',
    'http://www.w3.org/2004/02/skos/core#notation':'ChEBI_id'
}


chebi_df = chebi_df.rename(renamed_columns_chebi,axis=1)


mesh_df = pd.read_csv('data/MESH_with_Semantic_types.csv')
mesh_df


list(mesh_df.type.unique())


chemical_types = ['Inorganic Chemical',
         'Organic Chemical',
         'Immunologic Factor',
         'Indicator, Reagent, or Diagnostic Aid',
         'Biologically Active Substance',
         'Element, Ion, or Isotope',
         'Antibiotic',
         'Pharmacologic Substance',
         'Molecular Function',
         'Hazardous or Poisonous Substance',
         'Vitamin',
         'Chemical Viewed Structurally',
         'Chemical',
         'Clinical Attribute',
         'Hormone',
         'Clinical Drug',
         'Substance',
         'Body Substance',
         'Chemical Viewed Functionally',
         'Laboratory or Test Result'
 ]


chemical_mesh_df = mesh_df[mesh_df.type.isin(chemical_types)]


chemical_mesh_df.sample(10)


print(chemical_mesh_df.shape)


chemical_mesh_df.to_csv('data/MESH_chemicals.csv',index=False)


chemical_mesh_df = pd.read_csv('data/MESH_chemicals.csv')
chemical_mesh_df


df = pd.merge(pubchem_df,chebi_df,on='inchikey',how='outer')
df


df.isnull().sum(axis = 0)


df.fillna('', inplace=True)
df


(df['meshheadings'].values == '').sum() 


chemical_mesh_df.sample(10)


from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


import dask
from dask.distributed import Client, progress
from dask import delayed

client = Client()
client


@delayed(nout=2)
def check_mesh(chemical_mesh_df,i,pubchem_doc):
    for j in chemical_mesh_df.itertuples(index=False):
        if (j[1].lower() == i[1].lower()) or (j[1].lower in [item.lower() for item in pubchem_doc['synonyms']]):
            return True, j
    return False, j


@delayed(nout=4)
def mesh_addition(mesh_flag,mesh,pubchem_doc,new_syn_mesh,total_mesh_concepts): 
    if mesh_flag:
        mesh_id = mesh[0]
        print(mesh_id)
        if mesh[1] not in pubchem_doc['synonyms']:
            new_syn_mesh.append(mesh[1])
        semantic_type = mesh[6]
        total_mesh_concepts = total_mesh_concepts + 1
        return mesh_id,new_syn_mesh,semantic_type,total_mesh_concepts
    return '',[],'',total_mesh_concepts


documents = []
num = 0
total_mesh_concepts = 0

for i in df.itertuples(index=False):
    pubchem_doc = {}
    
    if len(i[1])==0 and len(i[5])>0:
            pubchem_doc['term'] = i[5]
    else:
            pubchem_doc['term'] = i[1]
            
    synonyms = []
    if len(i[2])>0:
        synonyms = synonyms + i[2].split("|")  
    if len(i[6])>0:
        possible_new_syn = i[6].split("|")
        if possible_new_syn not in synonyms:
            synonyms = synonyms + possible_new_syn
    pubchem_doc['synonyms']= synonyms
    
    mesh_id=''
    semantic_type =''
    new_syn_mesh = []
    
    mesh_flag,mesh = check_mesh(chemical_mesh_df,i,pubchem_doc)
    mesh_id,new_syn_mesh,semantic_type,total_mesh_concepts = mesh_addition(mesh_flag,mesh,pubchem_doc,new_syn_mesh,total_mesh_concepts)
    
    pubchem_doc['synonyms'] = pubchem_doc['synonyms'] + new_syn_mesh
    pubchem_doc['mesh_id'] = mesh_id
    pubchem_doc['semantic_type'] = semantic_type        
    pubchem_doc['cid']= i[0]
    pubchem_doc['chebi_id']= i[9] 
    pubchem_doc['inchikey'] = i[3]
    
    cross_references = []
    if len(i[9])>0:
        cross_references = cross_references + i[8].split("|")
    pubchem_doc['cross_references']= cross_references
    
    num +=1
    if numget_ipython().run_line_magic("1000", " == 0:")
        print(num/df.shape[0]*100,'% processed')
        
    documents.append(pubchem_doc)


results = dask.persist(*documents)

print(total_mesh_concepts, 'MESH ids added')    


import json
import os


def check_mesh(chemical_mesh_df,i,pubchem_doc):
    for j in chemical_mesh_df.itertuples(index=False):
        if (j[1].lower() == i[1].lower()) or (j[1].lower in [item.lower() for item in pubchem_doc['synonyms']]):
            return True, j
    return False, j


def mesh_addition(mesh_flag,mesh,pubchem_doc,new_syn_mesh,total_mesh_concepts): 
    if mesh_flag:
        mesh_id = mesh[0]
        if mesh[1] not in pubchem_doc['synonyms']:
            new_syn_mesh.append(mesh[1])
        semantic_type = mesh[6]
        total_mesh_concepts = total_mesh_concepts + 1
        return mesh_id,new_syn_mesh,semantic_type,total_mesh_concepts
    return '',[],'',total_mesh_concepts


documents = []
num = 0
total_mesh_concepts = 0
file_part = 'data/final/drugs_doc_1.json'
checkpoint_flag = True
length_file = 20000

for i in df.itertuples(index=False):
    num +=1
    if os.path.isfile(file_part):
        #print('Archivo',file_part,'encontrado')
        if numget_ipython().run_line_magic("length_file", " == 0:")
            part = int(num/length_file)
            print('Part number',part)        
            file_part = 'data/final/drugs_doc_'+str(part+1)+'.json'
        checkpoint_flag = True

    else:
        pubchem_doc = {}

        if len(i[1])==0 and len(i[5])>0:
                pubchem_doc['term'] = i[5]
        else:
                pubchem_doc['term'] = i[1]

        synonyms = []
        if len(i[2])>0:
            synonyms = synonyms + i[2].split("|")  
        if len(i[6])>0:
            possible_new_syn = i[6].split("|")
            if possible_new_syn not in synonyms:
                synonyms = synonyms + possible_new_syn
        pubchem_doc['synonyms']= synonyms

        mesh_id=''
        semantic_type =''
        new_syn_mesh = []

        mesh_flag,mesh = check_mesh(chemical_mesh_df,i,pubchem_doc)
        mesh_id,new_syn_mesh,semantic_type,total_mesh_concepts = mesh_addition(mesh_flag,mesh,pubchem_doc,new_syn_mesh,total_mesh_concepts)
        
        
        pubchem_doc['synonyms'] = pubchem_doc['synonyms'] + new_syn_mesh
        pubchem_doc['mesh_headings'] = i[4]
        pubchem_doc['mesh_id'] = mesh_id
        pubchem_doc['semantic_type'] = semantic_type        
        pubchem_doc['cid']= i[0]
        pubchem_doc['chebi_id']= i[9] 
        pubchem_doc['inchikey'] = i[3]

        cross_references = []
        if len(i[9])>0:
            cross_references = cross_references + i[8].split("|")
        pubchem_doc['cross_references']= cross_references

        if numget_ipython().run_line_magic("(length_file/10)", " == 0:")
            print(num)
            print(num/df.shape[0]*100,'% processed')
            
        documents.append(pubchem_doc)
        
        if numget_ipython().run_line_magic("length_file", " == 0:")
            part = int(num/length_file)
            print('Part number',part)
            file_part = 'data/final/drugs_doc_'+str(part)+'.json'
            if checkpoint_flag:
                print('Processing file with checkpoint')
                with open(file_part, 'w') as fout:
                    json.dump(documents, fout)
                start = length_file
                end = start + length_file
            else:
                print('Processing file without checkpoint from',start)
                with open(file_part, 'w') as fout:
                    json.dump(documents[start:end], fout)
                start = end
                end = start + length_file
            file_part = 'data/final/drugs_doc_'+str(part+1)+'.json'
            checkpoint_flag = False
    


print(total_mesh_concepts, 'MESH ids added')    


mypath = 'data/final'
drug_files = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]


drugs = []
for file in drug_files:
    with open(os.path.join(mypath,file), 'r') as fout:
        drug_file = json.loads(fout.read())
    drugs += drug_file

with open('data/drugs_doc_all.json', 'w') as fout:
    json.dump(drugs, fout)

print(len(drugs))


with open('data/drugs_doc_all.json', 'r') as fout:
    drug_file = json.loads(fout.read())
    print(len(drug_file))


drug_file[0].keys()


important_columns_ctd = ['ChemicalName','ChemicalID', 'Synonyms']


ctd_df = pd.read_csv(
    "data/CTD_chemicals.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_ctd,
    dtype=str
)

print(ctd_df.shape)
ctd_df.head()


ctd_df = ctd_df[ctd_df['ChemicalID'].str.startswith('MESH')]
print(ctd_df.shape)


ctd_df['ChemicalID'] = ctd_df['ChemicalID'].apply(lambda x: x.split(':')[1])


#ctd_df['Synonyms'] = ctd_df['Synonyms'].apply(lambda x: [syn for syn in str(x).split('|')])


ctd_df.sample(10)


i = 0
num = 0
mesh_appended= set([])

for doc in drug_file:
    if doc['mesh_id'] == '':
        proposed_mesh = ctd_df.loc[ctd_df['ChemicalName']==doc['term']]
        if len(proposed_mesh)>0:
            new_mesh = str(proposed_mesh['ChemicalID'].values[0])
            doc['mesh_id'] = new_mesh
            mesh_appended.add(new_mesh)
            num +=1
        else:
            try:
                alternative_proposed_mesh = ctd_df[ctd_df['Synonyms'].str.contains(doc['term'], na=False, case = False,regex= False)]
                #print(alternative_proposed_mesh)
                if len(alternative_proposed_mesh)>0 and len(alternative_proposed_mesh)<2:
                    new_mesh = str(alternative_proposed_mesh['ChemicalID'].values[0])
                    doc['mesh_id'] = new_mesh
                    mesh_appended.add(new_mesh)
                    num +=1
                elif len(alternative_proposed_mesh)>1:
                    all_terms=[]
                    for j in alternative_proposed_mesh.itertuples(index=False):
                        all_terms.append(j[0])
                        all_terms += [f for f in j[2].split('|')]
                        
                        if doc['term'] in all_terms:
                            doc['mesh_id'] = j[1]
                            mesh_appended.add(j[1])
                            num +=1
                        elif similar(doc['term'],j[0])>0.85:
                            doc['mesh_id'] = j[1] 
                            mesh_appended.add(j[1])
                            num +=1
                        all_terms=[]
                        
                
            except:
                print('Error in:',doc['term'])
            
    i +=1
    if iget_ipython().run_line_magic("1000", " == 0:")
        print(i)
        print('No of mesh ids linked:',num)
            


with open('data/drugs_with_mesh.json', 'w') as fout:
    json.dump(drug_file, fout)


with open('data/foo.txt', 'w') as fout:
    for row in mesh_appended:
        output.write(str(row) + '\n')


with open('data/drugs_with_mesh.json', 'r') as fout:
    drug_file = json.loads(fout.read())
    print(len(drug_file))


drug_file[0].keys()


with open('data/mesh_appended.txt','r') as f:
    mesh_appended = f.read().splitlines() 


len(mesh_appended)


df_not_linked = ctd_df[~ctd_df.ChemicalID.isin(mesh_appended)]


df_not_linked.fillna('',inplace=True)


df_not_linked


for i in df_not_linked.itertuples(index=False):
    new_doc = {}
    new_doc['term'] = i[0]
    
    synonyms = []
    if len(i[2])>0:
        synonyms += i[2].split("|")
    new_doc['synonyms']= synonyms

    new_doc['mesh_headings'] = ''
    new_doc['mesh_id'] = i[1]
    new_doc['semantic_type'] = ''
    new_doc['cid'] = ''
    new_doc['chebi_id'] = ''
    new_doc['inchikey'] = ''
    new_doc['cross_references'] = ''
    
    drug_file.append(new_doc)


print(len(drug_file))


from collections import Counter
counter = Counter(x['mesh_id'] for x in drug_file)


print('The number of terms with MESH value is:',len(drug_file)-counter[''])


with open('data/drugs.json', 'w') as fout:
    json.dump(drug_file, fout)


with open('data/drugs.json', 'r') as fout:
    drugs = json.loads(fout.read())
    print(len(drugs))


atc_df= pd.read_csv(
    "data/ATC.csv",      # relative python path to subdirectory
    sep=',',
    na_filter=False,
    quotechar="\"",       # single quote allowed as quote character
    usecols=['Class ID','Preferred Label','Synonyms','Semantic Types','ATC LEVEL'],
    dtype=str
)

print(atc_df.shape)
atc_df.head()


atc_df['Class ID'] = atc_df['Class ID'].apply(lambda x: x.split('/')[-1])


atc_df['Semantic Types'] = atc_df['Semantic Types'].apply(lambda x: x.split('|')[-1].split('/')[-1])


semantic_type_df = pd.read_csv(
    "data/STY.csv",      # relative python path to subdirectory
    sep=',',
    dtype=str,
    usecols=['Class ID','Preferred Label']
)
semantic_type_df


semantic_type_df['Class ID'] = semantic_type_df['Class ID'].apply(lambda x: x.split('/')[-1])


atc_df = atc_df.merge(semantic_type_df,left_on='Semantic Types',right_on='Class ID',how='inner')


atc_df.drop(['Semantic Types','Class ID_y'],inplace=True,axis=1)


atc_df.rename(columns={'Preferred Label_y':'Semantic Type','Preferred Label_x':'Term','Class ID_x':'ATC'},inplace=True)


atc_df


i = 0
num = 0
atc_appended= set([])

for doc in drugs:
    proposed_atc = atc_df.loc[atc_df['Term']==doc['term']]
    if len(proposed_atc)>0:
        new_atc_id = str(proposed_atc['ATC'].values[0])
        new_semantic_type = str(proposed_atc['Semantic Type'].values[0])
        new_level = str(proposed_atc['ATC LEVEL'].values[0])
        doc['ATC'] = new_atc_id
        doc['semantic_type'] = new_semantic_type
        doc['ATC_level'] = new_level
        atc_appended.add(new_atc_id)
        num +=1
    elif len(proposed_atc)<1:
        try:
            alternative_proposed_atc = atc_df[atc_df['Synonyms'].str.contains(doc['term'], na=False, case = False,regex= False)]
            if len(alternative_proposed_atc)>0 and len(alternative_proposed_atc)<2:
                new_atc_id = str(alternative_proposed_atc['ATC'].values[0])
                new_semantic_type = str(alternative_proposed_atc['Semantic Type'].values[0])
                new_level = str(alternative_proposed_atc['ATC LEVEL'].values[0])
                doc['ATC'] = new_atc_id
                doc['semantic_type'] = new_semantic_type
                doc['ATC_level'] = new_level
                atc_appended.add(new_atc_id)
                num +=1
            elif len(alternative_proposed_atc)>1:
                all_terms=[]
                for j in alternative_proposed_atc.itertuples(index=False):
                    all_terms.append(j[1])
                    all_terms += [f for f in j[2].split('|')]

                    if doc['term'] in all_terms:
                        doc['ATC'] = j[0]
                        doc['semantic_type'] = j[4]
                        doc['ATC_level'] =j[3]
                        atc_appended.add(j[0])
                        num +=1
                    elif similar(doc['term'],j[0])>0.92:
                        doc['ATC'] = j[0]
                        doc['semantic_type'] = j[4]
                        doc['ATC_level'] =j[3]
                        atc_appended.add(j[0])
                        num +=1
                    all_terms=[]
            else:
                doc['ATC'] = ''
                doc['ATC_level'] = ''
                    
        except:
            print('Error in:',doc['term'])


        
            
    i +=1
    if iget_ipython().run_line_magic("30000", " == 0:")
        print(i)
        print('No of ATC ids linked:',num)


len(atc_appended)


atc_appended=list(atc_appended)


df_not_linked = atc_df[~atc_df['ATC'].isin(atc_appended)]


df_not_linked.fillna('',inplace=True)


drugs[0]


df_not_linked


for i in df_not_linked.itertuples(index=False):
    new_doc = {}
    new_doc['term'] = i[1]
    
    synonyms = []
    if len(i[2])>0:
        synonyms += i[2].split("|")
    new_doc['synonyms']= synonyms

    new_doc['mesh_headings'] = ''
    new_doc['mesh_id'] = ''
    new_doc['semantic_type'] = i[4]
    new_doc['cid'] = ''
    new_doc['chebi_id'] = ''
    new_doc['inchikey'] = ''
    new_doc['cross_references'] = ''
    new_doc['ATC'] = i[0]
    new_doc['ATC_level'] = i[3]
    
    drugs.append(new_doc)


print(len(drugs))


with open('data/Chemical/drugs.json', 'w') as fout:
    json.dump(drugs, fout)


with open('data/Chemical/drugs.json', 'r') as fout:
    drugs = json.loads(fout.read())
    print(len(drugs))











import pysolr
import math

solr = pysolr.Solr('http://localhost:8983/solr/gettingstarted', timeout=10)


solr.ping()


import pysolr
import math

solr = pysolr.Solr('http://librairy.linkedddata.es/data/atc', timeout=2)

documents = []
num = 0;
for i in atc_data.itertuples(index=False):
    atc_document = {}
    if (len(i[0]) > 0 ):
        atc_document['id']= i[0].split("/")[-1]
    if (len(i[1]) > 0 ):
        atc_document['label_t']= i[1].lower()
    synonyms = []
    if (len(i[2]) > 0 ):
        synonyms.append(i[2].lower())
    atc_document['synonyms']= synonyms
    if (len(i[3]) > 0 ):
        atc_document['cui_s']= i[3]
    if (len(i[4]) > 0 ):
        atc_document['parent_s']= i[4].split("/")[-1]
    if (len(i[5]) > 0 ):
        atc_document['level_i']= i[5]    
    documents.append(atc_document) 
    num+=1
    if (len(documents) > 0 and len(documents) get_ipython().run_line_magic("100", " == 0):")
        solr.add(documents)
        solr.commit()
        documents = []
        print(num,"drugs added")


drugs_data = pd.read_csv(
    "data/drugs.csv",      # relative python path to subdirectory
    sep=';',           # Tab-separated value file.
    quotechar="\"",        # single quote allowed as quote character
    usecols=['Nº Registro', 'Medicamento', 'Cód. ATC']
)

print(drugs_data.shape) 
drugs_data.head()




atc_regs = drugs_data.groupby('Cód. ATC')['Nº Registro'].apply(list).reset_index()
print(atc_regs)


import pysolr

solr = pysolr.Solr('http://librairy.linkeddata.es/data/atc', timeout=2)

def add_num_regs(atc_code,num_regs):
    solr_query = "id:"+str(atc_code)
    for result in solr.search(q=solr_query,rows=1):
        result['cima_codes']=num_regs
        del (result['_version_'])
        return result
    return {}

sample_doc = {
  "id": "1212121212112121212",
  "cima_codes":["a","b","c"]
}
solr.add([sample_doc])
solr.commit()
solr.delete(id=sample_doc['id'])
solr.commit()
updated_drugs = []
num = 0;
for i in atc_regs.itertuples():
    atc_code = i[1]
    updated_drugs.append(add_num_regs(atc_code,[str(code) for code in i[2]])) 
    num+=1
    if (len(updated_drugs) == 0):
        solr.add(updated_drugs)
        solr.commit()
        updated_drugs = []
        print(num,"drugs updated")


import pandas as pd

md_df = pd.read_excel('data/Medicines_output_european_public_assessment_reports.xlsx', skiprows=8, keep_default_na=False)
md_df.head()


import pysolr
import html2text
import urllib.request

solr = pysolr.Solr('http://librairy.linkeddata.es/data/atc', timeout=20)

h = html2text.HTML2Text()
h.ignore_links = True

def add_field_in_list(resource,field,value):
    if (len(value) > 0 ):
        if (not field in resource):
            resource[field] = []
        resource[field].append(value)  

def get_txt(url):
    fp = urllib.request.urlopen(url)
    mybytes = fp.read()
    mystr = mybytes.decode("utf8")
    fp.close()
    return h.handle(mystr)

documents = []
i = 1
for row in md_df.itertuples():
    atc_code = row[9]
    if (len(atc_code) > 1):
        for drug in solr.search("id:"+atc_code):
            add_field_in_list(drug,'category',row[1])
            add_field_in_list(drug,'medicines',row[2])
            add_field_in_list(drug,'therapeutic_area',row[3])
            add_field_in_list(drug,'emea_codes',row[6])
            add_field_in_list(drug,'overviews',row[30])
            solr.add([drug])
            solr.commit()
            #print(drug)
            if (i % 100 == 0):
                print(i,"drugs updated")
            i+=1



