from bioprocessor import BioProcessor


class GeneProcessor(BioProcessor):

    def __init__(self, model_name):
        super().__init__(model_name)

    def normalize_gene_entities(self):
        pass
