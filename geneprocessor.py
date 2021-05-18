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

    def normalize_genetic_entities(self, genetic_ents):
        normalized_gen = []
        genetics = unique_terms(genetic_ents)
        for gen in genetics:
            label = re.sub(r'\W+', ' ', str(gen))
            solr_query = "term:\"" + label + "\"^100 or synonyms:\"" + label + "\"^10"
            results = self.solr_engine.search(solr_query)
            if len(results) < 1:
                genetics = {'text_term': gen}
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
                if 'cross_references' in result:
                    genetic['cross_references'] = result["cross_references"]
                if 'uniprot_id' in result:
                    genetic['uniprot_id'] = result["uniprot_id"]
                normalized_gen.append(genetic)
                break
        return normalized_gen
