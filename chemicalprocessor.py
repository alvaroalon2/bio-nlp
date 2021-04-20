from bioprocessor import BioProcessor


class ChemicalProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)

    def normalize_disease_entities(self):
        pass
