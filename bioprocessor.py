from transformers import AutoTokenizer, AutoModel, AutoConfig, AutoModelForTokenClassification, \
    TokenClassificationPipeline


class BioProcessor:
    def __init__(self, model_name):
        self.model_name = model_name
        self.config = AutoConfig.from_pretrained(self.model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_name,
            use_fast=True,
            return_offsets_mapping=True
        )
        self.model = AutoModelForTokenClassification.from_pretrained(
            self.model_name,
            config=self.config
        )

        self.pipeline = TokenClassificationPipeline(model=self.model, tokenizer=self.tokenizer, framework='pt',
                                                    task='ner',
                                                    grouped_entities=True)

        self.sequence = ''
        self.offset = 0
        self.results = {}

    def sentence_to_process(self, sequence):
        self.sequence = sequence
    
    def set_offset(self, offset):
        self.offset += offset

    def predict(self):
        self.results = self.pipeline(self.sequence)
        if self.offset > 0:
            for result in self.results:
                result['start'] += self.offset
                result['end'] += self.offset
        return self.results
