from bioprocessor import BioProcessor
import pysolr
from utils import unique_terms
import re


class ChemicalProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)
        try:
            self.solr_engine = pysolr.Solr('http://localhost:8983/solr/drugs', timeout=20)
        except ConnectionError:
            print('Connection with Solr Chemical database could not be established')

    def normalize_chemical_entities(self, chemical_ents):
        normalized_chems = []
        chems = unique_terms(chemical_ents)
        for chem in chems:
            label = re.sub(r'\W+', ' ', str(chem))
            # print(label)
            solr_query = "term:\"" + label + "\"^100 or synonyms:\"" + label + "\"^10 or mesh_headings:\"" + label + "\"^5"
            results = self.solr_engine.search(solr_query)
            if len(results) < 1:
                # print('Non results for:', chem)
                chemical = {'text_term': chem}
                normalized_chems.append(chemical)
            for result in results:
                chemical = {}
                chemical["text_term"] = label
                chemical["found_term"] = "".join(result["term"])
                if 'cid' in result:
                    chemical['cid'] = result["cid"]
                if 'mesh_id' in result:
                    chemical['mesh_id'] = result["mesh_id"]
                if 'chebi_id' in result:
                    chemical['chebi_id'] = result["chebi_id"]
                if 'cross_references' in result:
                    chemical['cross_references'] = result["cross_references"]
                if 'ATC' in result:
                    chemical['ATC'] = "".join(result["ATC"])
                if 'ATC_level' in result:
                    chemical['ATC_level'] = result["ATC_level"]
                normalized_chems.append(chemical)
                break
        return normalized_chems
