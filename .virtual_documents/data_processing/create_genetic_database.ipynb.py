import pandas as pd 
import numpy as np


def set_term(column1,column2):
    if not pd.isnull(column1):
        return column1
    if not pd.isnull(column2):
        return column2
    else:
        return np.nan


def separate_id(column,idx):
    for i in column:
        if idx in i:
            column.remove(i)
            if idx == 'UniProtKB' or idx == 'NCBIGene':
                return i.split(':')[-1]
            return i
    return np.nan


important_columns_ogg = ['Preferred Label','Synonyms','Obsolete','database_cross_reference','full name from nomenclature authority','type of gene','NCBI GeneID','organism NCBITaxon ID']


ogg_df = pd.read_csv('data/Genetic/OGG.csv', usecols=important_columns_ogg, dtype=str)
ogg_df


ogg_df = ogg_df[ogg_df['Obsolete']=='false']
ogg_df.drop('Obsolete',inplace=True,axis=1)
print(ogg_df.shape)


ogg_df


important_columns_pr = ['Preferred Label','Synonyms','Obsolete','database_cross_reference','http://www.geneontology.org/formats/oboInOwl#id','only_in_taxon','http://www.w3.org/2000/01/rdf-schema#comment']


pr_df = pd.read_csv('data/Genetic/PR.csv', usecols=important_columns_pr, dtype=str)
pr_df


pr_df = pr_df[pr_df['Obsolete']=='false']
pr_df.drop('Obsolete',inplace=True,axis=1)
print(pr_df.shape)


renamed_pr = {'Preferred Label':'Term','database_cross_reference':'cross_reference','http://www.geneontology.org/formats/oboInOwl#id':'id','only_in_taxon':'taxon_id','http://www.w3.org/2000/01/rdf-schema#comment':'type_of_ent'}
pr_df = pr_df.rename(columns=renamed_pr)


pr_df['taxon_id'] = pr_df['taxon_id'].apply(lambda x: x.split('/')[-1].split('_')[-1] if isinstance(x,str) else np.nan)


pr_df['type_of_ent'] = pr_df['type_of_ent'].apply(lambda x: x[x.find('Category=') + len('Category='):] if isinstance(x,str) else np.nan)


pr_df['type_of_ent'] = pr_df['type_of_ent'].apply(lambda x: x.split('.')[0] if isinstance(x,str) else np.nan)


pr_df['type_of_ent'] = pr_df['type_of_ent'].apply(lambda x: x.replace('-',' ') if isinstance(x,str) else np.nan)


pr_df['cross_reference'] = pr_df['cross_reference'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan)


pr_df['UniprotID'] = pr_df.apply(lambda x: separate_id(x.cross_reference,'UniProtKB') if isinstance(x.cross_reference,list) else np.nan,axis=1)


pr_df['NCBI_id'] = pr_df['id'].apply(lambda x: x.split(':')[-1] if x.startswith('NCBIGene') else np.nan)


pr_df.count()


pr_df.sample(10)


pr_df.rename(columns={'NCBI_id':'NCBI GeneID'},inplace=True)


pr_df


df1 = pd.concat([pr_df,ogg_df])
df1


df1 = df1[(~df1.duplicated(subset='NCBI GeneID',keep='last')) | (df1['NCBI GeneID'].isnull())]


df1


df1['Term'] = df1.apply(lambda x: set_term(x['Preferred Label'],x.Term),axis=1)
df1.drop('Preferred Label',axis=1,inplace=True)


df1['Synonyms'] = df1.apply(lambda x: set_term(x.Synonyms,x['full name from nomenclature authority']),axis=1)
df1.drop('full name from nomenclature authority',axis=1,inplace=True)


df1['cross_reference'] = df1['cross_reference'].apply(lambda x: np.nan if x == [] else x)


df1['NCBItaxon_id'] = df1.apply(lambda x: set_term(x['taxon_id'],x['organism NCBITaxon ID']),axis=1)
df1.drop(['organism NCBITaxon ID','taxon_id'],axis=1,inplace=True)


df1['cross_reference'] = df1['cross_reference'].apply(lambda d: d if isinstance(d, list) else [])


df1['database_cross_reference'] = df1['database_cross_reference'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan )


df1['database_cross_reference'] = df1['database_cross_reference'].apply(lambda d: d if isinstance(d, list) else [])


df1['id'] = df1['id'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan )


df1['id'] = df1['id'].apply(lambda d: d if isinstance(d, list) else [])


df1['cross_reference'] = df1['cross_reference'] + df1['database_cross_reference'] + df1['id']


df1.drop(['database_cross_reference','id'],axis=1,inplace=True)


df1.sample(10)


df1['type of gene'] = df1['type of gene'].apply(lambda d: d + ' gene' if isinstance(d, str) else np.nan)


df1['type'] = df1.apply(lambda x: set_term(x.type_of_ent,x['type of gene']),axis=1)
df1.drop(['type of gene','type_of_ent'],axis=1,inplace=True)


df1['Synonyms'] = df1['Synonyms'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else np.nan )


df1


df1.count()


ctd_df = pd.read_csv('data/Genetic/CTD_genes.csv', dtype=str)
ctd_df


ctd_df.drop('AltGeneIDs',axis=1,inplace=True)


ctd_df['Synonyms'] = ctd_df['Synonyms'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else [])


ctd_df['GeneName'] = ctd_df['GeneName'].apply(lambda x: [i for i in x.split('|')] if isinstance(x,str) else [])


ctd_df['Synonyms'] = ctd_df['Synonyms'] + ctd_df['GeneName']


ctd_df.drop('GeneName',axis=1,inplace=True)


ctd_df['BioGRIDIDs'] = ctd_df['BioGRIDIDs'].apply(lambda x: ['BioGRIDIDs:' + i for i in x.split('|')] if isinstance(x,str) else [])


ctd_df['PharmGKBIDs'] = ctd_df['PharmGKBIDs'].apply(lambda x: ['PharmGKBIDs:' + i for i in x.split('|')] if isinstance(x,str) else [])


ctd_df['cross_reference'] = ctd_df['BioGRIDIDs'] + ctd_df['PharmGKBIDs']
ctd_df.drop(['PharmGKBIDs','BioGRIDIDs'],axis=1,inplace=True)


ctd_df.rename(columns={'GeneID':'NCBI GeneID','UniProtIDs':'UniprotID'},inplace=True)


ctd_df.sample(10)


df_final = pd.concat([ctd_df,df1])
df_final


df_final = df_final[(~df_final.duplicated(subset='NCBI GeneID',keep='last')) | (df_final['NCBI GeneID'].isnull())]


df_final['Term'] = df_final.apply(lambda x: set_term(x.Term,x['GeneSymbol']),axis=1)
df_final.drop('GeneSymbol',axis=1,inplace=True)


df_final.count()


df_final.sample(10)





df_final.fillna('',inplace=True)


df_final.sample(5)


documents = []
num = 0;
for i in df_final.itertuples(index=False):
    gene_document = {}
    gene_document['term'] = i[4]
    gene_document['synonyms']= i[1]
    gene_document['uniprot_id']= i[2]
    gene_document['ncbi_gene_id']= i[0]
    gene_document['ncbi_taxon_id'] = i[5]
    gene_document['type']= i[6]
    gene_document['cross_reference']= i[3]   
    documents.append(gene_document) 
    num+=1
    if numget_ipython().run_line_magic("50000", " == 0:")
        print(num/len(df_final)*100,'% genes processed')


len(documents)


documents[22222]


with open('data/Genetic/genetic.json', 'w') as fout:
    json.dump(documents, fout)


with open('data/Genetic/genetic.json', 'r') as fout:
    genetic = json.loads(fout.read())
    print(len(genetic))



