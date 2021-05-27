# Solr Creation
In order to ease the creation of the proposed Solr along with its cores and configuration the following process is recommended to follow:
 1. `docker-compose up` on this folder for creating a Solr 8 instance along with the proposed cores.
 2. Give execution permissions to *create_schemas.sh*: `sudo chmod +x ./create_schemas.sh`
 3. Execute the creation of schemas: `./create_schemas.sh`

This will create the proposed Solr system in *localhost*. For deploying it on a desired host some modifications will have to be done in these files for adapting the host.

## Retrieved terms
The retrieved terms used for populating the SSolr cores will be obtained through the execution of each of the *./data_processing/create_xxxx_database.ipynb* where xxxx is each of the entities which will be procesed.
Therefore, a processing of terms will be done for each of the entities the system will be focused on.

## Populate Solr Database
In order to populate the Solr database, the Notebook [*Solr_indexing*](https://github.com/alvaroalon2/bio-nlp/blob/master/Solr/data_processing/Solr_indexing.ipynb) is used.
Its complete execution will populate each of the cores for each of the entity classes.

pysolr library is required for the use of Solr through Python.
