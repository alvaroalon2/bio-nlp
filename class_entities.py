
class Entities:

    def __init__(self,text, diseases, chemicals, genes):
        self.text = text
        self.diseases = diseases
        self.chemicals = chemicals
        self.genes = genes

        self.ents = self.diseases + self.chemicals + self.genes

        self.ents_sorted = []

    def get_diseases_entities(self):
        return self.diseases

    def get_chemicals_entities(self):
        return self.chemicals

    def get_genes_entities(self):
        return self.genes
    
    def append_new_entities(self, entities):
        self.ents += entities
        
    def sort_entities(self):
        self.ents_sorted = sorted(self.ents, key=lambda k: k['start'])

    def parse_ner_spacy(self):
        self.sort_entities()
        dict_results = {
            "text": self.text,
            "ents": [{"start": ent['start'],
                      "end": ent['end'],
                      "label": ent['entity_group']
                      } for ent in self.ents_sorted if ent['entity_group'] != '0'],
            "title": None
        }
        return dict_results
