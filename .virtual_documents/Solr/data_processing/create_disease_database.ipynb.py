import pandas as pd 
import numpy as np


def set_term(column1,column2):
    if not pd.isnull(column1):
        return column1
    if not pd.isnull(column2):
        return column2
    else:
        return np.nan


def set_term_list(column1,column2):
    if isinstance(column1,list):
        return column1
    if isinstance(column2,list):
        return column2
    else:
        return np.nan


def separate_id(column,idx):
    for i in column:
        if idx in i:
            column.remove(i)
            if idx == 'MESH' or idx == 'UMLS_CUI':
                return i.split(':')[-1]
            return i
    return np.nan


mesh_df = pd.read_csv('data/MESH_with_Semantic_types.csv')
mesh_df


disease_types = ['Indicator, Reagent, or Diagnostic Aid',
 'Virus',
 'Bacterium',
 'Disease or Syndrome',
 'Diagnostic Procedure',
 'Congenital Abnormality',
 'Therapeutic or Preventive Procedure',
 'Pathologic Function',
 'Health Care Activity',
 'Injury or Poisoning',
 'Finding',
 'Neoplastic Process',
 'Mental or Behavioral Dysfunction',
 'Organ or Tissue Function',
 'Anatomical Abnormality',
 'Cell Function',
 'Genetic Function',
 'Phenomenon or Process',
 'Physiologic Function',
 'Sign or Symptom',
 'Mental Process',
 'Cell or Molecular Dysfunction',
 'Acquired Abnormality',
 'Experimental Model of Disease',
 'Biologic Function',
 'Behavior',
 ]


disease_mesh_df = mesh_df[mesh_df.type.isin(disease_types)]
disease_mesh_df


print(disease_mesh_df.shape)


disease_mesh_df.rename(columns={'Class ID':'MeshID'},inplace=True)


important_columns_ctd = {
    'DiseaseName',
    'DiseaseID',
    'AltDiseaseIDs',
    'Synonyms',
    'SlimMappings'
}


ctd_disease_df = pd.read_csv(
    "data/Disease/CTD_diseases.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_ctd,
    dtype=str
)

print(ctd_disease_df.shape)
ctd_disease_df.head()


ctd_disease_df['MeshID'] = ctd_disease_df['DiseaseID'].apply(lambda x: x if x.startswith('MESH') else '')


ctd_disease_df['Cross_reference'] = ctd_disease_df['DiseaseID'].apply(lambda x: x if not x.startswith('MESH') else '')


ctd_disease_df['MeshID'] = ctd_disease_df['MeshID'].apply(lambda x: x.split(':')[-1])


ctd_disease_df['AltDiseaseIDs'] = ctd_disease_df['AltDiseaseIDs'].apply(lambda x: [f for f in x.split('|')] if isinstance(x,str) else np.nan)


ctd_disease_df['Cross_reference'] = ctd_disease_df['Cross_reference'].replace('',np.nan)


ctd_disease_df['Cross_reference'] = ctd_disease_df['Cross_reference'].apply(lambda x: [x] if isinstance(x,str) else [])


ctd_disease_df['Cross_reference'] = ctd_disease_df['Cross_reference'] + ctd_disease_df['AltDiseaseIDs']


ctd_disease_df.drop(['DiseaseID','AltDiseaseIDs'], axis=1, inplace=True)


ctd_disease_df


df_merge = disease_mesh_df.merge(ctd_disease_df,on='MeshID',how='outer')


df_merge.sample(10)


df_merge['Term'] = df_merge.apply(lambda x: set_term(x['Preferred Label'],x.DiseaseName),axis=1)


df_merge.drop(['DiseaseName','Preferred Label'],axis=1, inplace=True)


df_merge['Synonyms'] = df_merge.apply(lambda x: set_term(x.Synonyms_x,x.Synonyms_y),axis=1)


df_merge.drop(['Synonyms_x','Synonyms_y'],axis=1, inplace=True)


df_merge['type'] = df_merge.apply(lambda x: set_term(x.SlimMappings,x.type),axis=1)


df_merge.drop(['SlimMappings'],axis=1, inplace=True)


df_merge


important_columns_doid = {
    'Preferred Label',
    'Synonyms',
    'Obsolete',
    'database_cross_reference',
    'id'
}


doid_df = pd.read_csv(
    "data/Disease/DOID.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_doid,
    dtype=str
)

print(doid_df.shape)
doid_df.head()


doid_df = doid_df[doid_df['Obsolete']=='false']
print(doid_df.shape)


doid_df = doid_df.drop(['Obsolete'],axis=1)


doid_df['database_cross_reference'] = doid_df['database_cross_reference'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan)


doid_df


doid_df['MeshID'] = doid_df.apply(lambda x: separate_id(x.database_cross_reference,'MESH') if isinstance(x.database_cross_reference,list) else np.nan,axis=1)


doid_df['CUI'] = doid_df.apply(lambda x: separate_id(x.database_cross_reference,'UMLS_CUI') if isinstance(x.database_cross_reference,list) else np.nan,axis=1)


doid_df


df1 = df_merge.merge(doid_df, on='MeshID', how='outer')


df1


len(df1[df1['CUI_x'].str.len()>0])


len(df1[df1['CUI_y'].str.len()>0])


df1['CUI'] = df1.apply(lambda x: set_term(x.CUI_x,x.CUI_y),axis=1)


len(df1[df1['CUI'].str.len()>0])


df1['Synonyms_x'] = df1['Synonyms_x'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan)


df1['Synonyms_y'] = df1['Synonyms_y'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan)


df1['Synonyms_x'] = df1['Synonyms_x'].apply(lambda x: x if isinstance(x,list) else [])
df1['Synonyms_y'] = df1['Synonyms_y'].apply(lambda x: x if isinstance(x,list) else [])


df1['Synonyms'] = df1['Synonyms_x'] + df1['Synonyms_y']


df1['Synonyms'] = df1['Synonyms'].apply(lambda x: x if len(x)> 0 else np.nan )


df1['Term'] = df1.apply(lambda x: set_term(x.Term,x['Preferred Label']),axis=1)


df1.drop(['CUI_x','Synonyms_x','Preferred Label','Synonyms_y','CUI_y'],axis=1,inplace=True)


df1


df1['database_cross_reference'] = df1['database_cross_reference'].apply(lambda x: x if isinstance(x, list) else [])


df1['Cross_reference'] = df1['Cross_reference'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else [])


df1['id'] = df1['id'].apply(lambda x: [x] if isinstance(x,str) else [])


df1['cross_reference'] = df1['database_cross_reference'] + df1['Cross_reference'] + df1['id']


df1['cross_reference'] = df1['cross_reference'].apply(lambda x: np.nan if x ==[] else x)


df1.drop(['database_cross_reference','id','Cross_reference'],axis=1,inplace=True)


df1.sample(10)


important_columns_icd = ['Class ID','Preferred Label','Synonyms','Obsolete','CUI','Semantic Types']


icd_df = pd.read_csv(
    "data/Disease/ICD10CM.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_icd,
    dtype=str
)

print(icd_df.shape)
icd_df.head()


icd_df = icd_df[icd_df['Obsolete']=='false']
icd_df.drop('Obsolete',inplace=True,axis=1)
print(icd_df.shape)


icd_df['ICD_id'] = icd_df['Class ID'].apply(lambda x: x.split('/')[-1])
icd_df.drop('Class ID',inplace=True,axis=1)


icd_df['Semantic Types'] = icd_df['Semantic Types'].apply(lambda x: x.split('/')[-1] if isinstance(x,str) else np.nan)


icd_df


semantic_type_df = pd.read_csv(
    "data/STY.csv",      # relative python path to subdirectory
    sep=',',
    dtype=str,
    usecols=['Class ID','Preferred Label']
)
semantic_type_df


semantic_type_df.rename(columns={'Class ID': 'Semantic Types','Preferred Label':'type'}, inplace=True)


semantic_type_df['Semantic Types'] = semantic_type_df['Semantic Types'].apply(lambda x: x.split('/')[-1])


semantic_type_df


icd_df = pd.merge(icd_df,semantic_type_df,on='Semantic Types',how='inner')


icd_df.drop('Semantic Types',inplace=True,axis=1)


icd_df['Synonyms'] = icd_df['Synonyms'].apply(lambda x: [f for f in x.split('|')] if isinstance(x,str) else np.nan)


icd_df


df = pd.merge(icd_df,df1,on='CUI',how='outer')


df


df['Term'] = df.apply(lambda x: set_term(x['Preferred Label'],x.Term),axis=1)


df['Synonyms'] = df.apply(lambda x: set_term_list(x.Synonyms_x,x.Synonyms_y),axis=1)


df['type'] = df.apply(lambda x: set_term(x.type_y,x.type_x),axis=1)


df.drop(['Preferred Label','Synonyms_x','Synonyms_y','type_y','type_x'],inplace=True,axis=1)


df['CUI'] = df['CUI'].apply(lambda x: [f for f in x.split('|')] if isinstance(x,str) else np.nan)


df.sample(10)


df.count()


df.fillna('',inplace=True)


documents = []
num = 0;
for i in df.itertuples(index=False):
    disease_document = {}
    disease_document['term'] = i[3]
    disease_document['synonyms']= i[5]
    disease_document['mesh_id']= i[2]
    disease_document['cui']= i[0]
    disease_document['ICD10_id'] = i[1]
    disease_document['semantic_type']= i[6]
    disease_document['cross_references']= i[4]   
    documents.append(disease_document) 
    num+=1
    if numget_ipython().run_line_magic("10000", " == 0:")
        print(num/len(df)*100,'% diseases processed')


len(documents)


documents[537]


with open('data/Disease/diseases.json', 'w') as fout:
    json.dump(documents, fout)


with open('data/Disease/diseases.json', 'r') as fout:
    diseases = json.loads(fout.read())
    print(len(diseases))



