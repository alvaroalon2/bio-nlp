from bioprocessor import BioProcessor
import pysolr
from utils import unique_terms
import re


class DiseaseProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)
        try:
            self.solr_engine = pysolr.Solr('http://localhost:8983/solr/diseases', timeout=20)
        except ConnectionError:
            print('Connection with Solr Diseases database could not be established')

    def normalize_disease_entities(self, disease_ents):
        normalized_dis = []
        diseases = unique_terms(disease_ents)
        for dis in diseases:
            label = re.sub(r'\W+', ' ', str(dis))
            solr_query = "term:\"" + label + "\"^100 or synonyms:\"" + label + "\"^10"
            results = self.solr_engine.search(solr_query)
            if len(results) < 1:
                disease = {'text_term': dis}
                normalized_dis.append(disease)
            for result in results:
                disease = {}
                disease["text_term"] = label
                disease["found_term"] = "".join(result["term"])
                if 'cui' in result:
                    disease['cui'] = result["cui"]
                if 'mesh_id' in result:
                    disease['mesh_id'] = result["mesh_id"]
                if 'ICD10_id' in result:
                    disease['ICD10_id'] = result["ICD10_id"]
                if 'cross_references' in result:
                    disease['cross_references'] = result["cross_references"]
                if 'semantic_type' in result:
                    disease['semantic_type'] = "".join(result["semantic_type"])
                normalized_dis.append(disease)
                break
        return normalized_dis
