import pandas as pd
import numpy as np


drug_targets = pd.read_csv('data/COVID/drug_target.csv')
drug_targets


drug_targets['evidence_url'] = drug_targets['Evidence'].apply(lambda x: x[x.find('https:'):-1])


drug_targets.iloc[0].evidence_url


drug_targets['Term'] = drug_targets['Target'].apply(lambda x: x[:x.find(' (')])


drug_targets['target_url'] = drug_targets['Target'].apply(lambda x: 'https://' + x[x.find('www.target'):-1])


drug_targets.iloc[0].target_url


drug_targets['ebi_reference'] = drug_targets['Cross-references'].apply(lambda x: x[x.find('https://'):x.find(') On')])


drug_targets.iloc[432].ebi_reference


drug_targets = drug_targets.drop(['Evidence','Cross-references','Target'],axis=1)


drug_targets


prot = pd.read_csv('data/COVID/proteins_covid.tsv', delimiter='\t')
prot.head()


prot['Term'] = prot['PRO Name '].apply(lambda x: x[:x.find('(SARS')])


prot.columns


prot['PR_id'] = prot['PRO ID ']


prot = prot.drop(['PRO ID ','PRO Name ','PRO Term Definition ','Category ','Parent '],axis=1)


prot


df = pd.concat([drug_targets,prot])
df


df = df.replace(np.nan,'')


documents = []
for i in df.itertuples(index=False):
    doc = {}
    doc['term'] = i[2]
    doc['association_score']= i[0]
    doc['evidence_url']= i[1]
    doc['target_url']= i[3]
    doc['ebi_reference']= i[4]
    doc['PR_id']= i[5]
    documents.append(doc) 



import json


with open('data/COVID/covid.json', 'w') as fout:
    json.dump(documents, fout)


with open('data/COVID/covid.json', 'r') as fout:
    covid = json.loads(fout.read())
    print(len(covid))






