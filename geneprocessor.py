from bioprocessor import BioProcessor
import pysolr
from utils import unique_terms
import re


class GeneProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)
        try:
            self.solr_engine = pysolr.Solr('http://localhost:8983/solr/genetic', timeout=20)
        except ConnectionError:
            print('Connection with Solr Genetic database could not be established')
        try:
            self.solr_engine_covid = pysolr.Solr('http://localhost:8983/solr/covid', timeout=20)
        except ConnectionError:
            print('Connection with Solr COVID database could not be established')

    def normalize_genetic_entities(self, genetic_ents):
        normalized_gen = []
        genetics = unique_terms(genetic_ents)
        for gen in genetics:
            label = re.sub(r'\W+', ' ', str(gen))
            solr_query = "term:\"" + label + "\"^100 or synonyms:\"" + label + "\"^10"
            results = self.solr_engine.search(solr_query)
            if len(results) < 1:
                solr_query = 'term:'+ label +'^100' + 'or synonyms:' + label+'^10'
                results = self.solr_engine.search(solr_query)
                if len(results) < 1:
                    genetics = {'text_term': label}
                    normalized_gen.append(genetics)
            for result in results:
                genetic = {}
                genetic["text_term"] = label
                genetic["found_term"] = "".join(result["term"])
                if 'ncbi_gene_id' in result:
                    genetic['ncbi_gene_id'] = result["ncbi_gene_id"]
                if 'ncbi_taxon_id' in result:
                    genetic['ncbi_taxon_id'] = result["ncbi_taxon_id"]
                if 'type' in result:
                    genetic['type'] = result["type"]
                if 'cross_reference' in result:
                    genetic['cross_reference'] = result["cross_reference"]
                if 'uniprot_id' in result:
                    genetic['uniprot_id'] = result["uniprot_id"]
                normalized_gen.append(genetic)
                break
        return normalized_gen

    def normalize_covid_entities(self, covid_ents):
        normalized_covid = []
        covid = unique_terms(covid_ents)
        for cov in covid:
            label = re.sub(r'\W+', ' ', str(cov))
            # print(label)
            solr_query = "term:\"" + label + "\""
            results = self.solr_engine_covid.search(solr_query)
            if len(results) < 1:
                # print('Non results for:', chem)
                covid_dict = {'text_term': label}
                normalized_covid.append(covid_dict)
            for result in results:
                covid_dict = {}
                covid_dict["text_term"] = label
                covid_dict["found_term"] = "".join(result["term"])
                if 'evidence_url' in result:
                    covid_dict['evidence_url'] = result["evidence_url"]
                if 'target_url' in result:
                    covid_dict['target_url'] = result["target_url"]
                if 'association_score' in result:
                    covid_dict['association_score'] = result["association_score"]
                if 'ebi_reference' in result:
                    covid_dict['ebi_reference'] = result["ebi_reference"]
                if 'PR_id' in result:
                    covid_dict['PR_id'] = "".join(result["PR_id"])
                normalized_covid.append(covid_dict)
                break
        return normalized_covid
