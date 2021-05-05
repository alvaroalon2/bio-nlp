from bioprocessor import BioProcessor

class ChemicalProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)
        #self.atc_solr = pysolr.Solr('http://localhost:8983/data/drugs', timeout=20)

    def normalize_chemical_entities(self):
        pass
