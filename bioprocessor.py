from transformers import AutoTokenizer, AutoModel, AutoConfig, AutoModelForTokenClassification, \
    TokenClassificationPipeline
import pysolr


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

    def set_offset(self, offset, restart=False):
        if restart:
            self.offset = 0
        else:
            self.offset = offset

    def predict(self):
        results = self.pipeline(self.sequence)
        if self.offset > 0:
            for result in results:
                result['start'] += self.offset
                result['end'] += self.offset
        self.results = results
        return self.results

