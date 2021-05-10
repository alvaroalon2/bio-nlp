import pandas as pd
import numpy as np


important_columns_mesh = [
    'Class ID',
    'Preferred Label',
    'Synonyms',
    'Obsolete',
    'Semantic Types',
    'CUI'
]


mesh_df = pd.read_csv(
    "data/MESH.csv",      # relative python path to subdirectory
    sep=',',
    usecols=important_columns_mesh,
    dtype=str
)
mesh_df


mesh_df['Class ID'] = mesh_df['Class ID'].apply(lambda x: x.split('/')[-1])


print(mesh_df.shape)


mesh_df = mesh_df[mesh_df['Obsolete']=='false']
mesh_df = mesh_df.drop('Obsolete',axis=1)
print(mesh_df.shape)


mesh_df[['ST1','ST2']] = mesh_df['Semantic Types'].str.split('|', 1, expand=True)
mesh_df[['ST2','ST3']] = mesh_df['ST2'].str.split('|', 1, expand=True)
mesh_df


mesh_df = mesh_df.drop('Semantic Types',axis=1)


mesh_df.sample(10)


def set_specific_type(type_x,type_y,type_z):
    if not pd.isnull(type_z):
        return type_z
    if not pd.isnull(type_y):
        return type_y
    return type_x


mesh_df['ST'] = mesh_df.apply(lambda x: set_specific_type(x.ST1,x.ST2,x.ST3),axis=1)


mesh_df = mesh_df.drop(['ST1','ST2','ST3'],axis=1)
mesh_df


semantic_type_df = pd.read_csv(
    "data/STY.csv",      # relative python path to subdirectory
    sep=',',
    dtype=str,
    usecols=['Class ID','Preferred Label']
)
semantic_type_df


semantic_type_df.rename(columns={'Class ID': 'Semantic_type_url','Preferred Label':'type'}, inplace=True)


mesh_df = pd.merge(mesh_df,semantic_type_df,left_on='ST',right_on='Semantic_type_url',how='inner')


print(mesh_df.shape)


mesh_df.sample(10)


mesh_df.columns


mesh_df= mesh_df[['Class ID','Preferred Label','Synonyms','CUI','type']]


mesh_df.sample(10)


mesh_df.isnull().sum(axis = 0)


mesh_df.to_csv('data/MESH_with_Semantic_types.csv',index=False)
