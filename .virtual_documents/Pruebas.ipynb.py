import spacy
from spacy import displacy

import bioprocessor
import chemicalprocessor
import diseaseprocessor
import geneprocessor
import class_entities
from utils import paragraphs
from spacy import util


import re


colors = {"DISEASE": "linear-gradient(90deg, #aa9cfc, #fc9ce7)",
          "CHEMICAL": "linear-gradient(90deg, #ffa17f, #3575ad)",
          "GENETIC": "linear-gradient(90deg, #c21500, #ffc500)"}


disease_service = diseaseprocessor.DiseaseProcessor('./models/Disease')
print('Disease Model Loaded')

chemical_service = chemicalprocessor.ChemicalProcessor('./models/Chemical')
print('Chemical Model Loaded')

genetic_service = geneprocessor.GeneProcessor('./models/Gene')
print('Genetic Model Loaded')


with open('prueba_comb.txt','r') as f:
    sequence = f.read()
    #print(sequence)


def process_by_paragraph(doc, entities):
    offset = 0
    for paragraph in paragraphs(doc):
        # print(len(str(paragraph)))
        disease_service.sentence_to_process(str(paragraph))
        disease_results = disease_service.predict()
        entities.append_new_entities(disease_results)
        chemical_service.sentence_to_process(str(paragraph))
        chemical_results = chemical_service.predict()
        entities.append_new_entities(chemical_results)
        genetic_service.sentence_to_process(str(paragraph))
        genetic_results = genetic_service.predict()
        entities.append_new_entities(genetic_results)

        entities.remove_non_entities()

        offset += len(str(paragraph))
        disease_service.set_offset(offset)
        chemical_service.set_offset(offset)
        genetic_service.set_offset(offset)

    disease_service.set_offset(0, restart=True)
    chemical_service.set_offset(0, restart=True)
    genetic_service.set_offset(0, restart=True)



from spacy.language import Language
nlp = spacy.load("en_core_web_sm", exclude=["tok2vec"])

@Language.component('ner_custom')
def ner_custom(doc):
    entities = class_entities.Entities(doc)
    process_by_paragraph(doc, entities)
    entities.postprocessing()
    print((len(entities)))
    return entities.doc

nlp.add_pipe('ner_custom', name='ner_custom',before='ner')


pattern_sars = r"((sarsr?|mers)(\s?\-?\s?(covs?))?(\s?\-?\s?2)?(\s?\binfe.{1,10}?\b)?)"
pattern_covid = r"((covid)(\s?\-?\s?(19))?(\s?\binfe.{1,10}?\b)?)"
pattern_coronavirus = r"((coronavir.{0,6}?\b)(\s?\bpneumo.{0,8}?\b)?(\s?\binfe.{1,10}?\b)?(\s?\bdiseas.{1,6}?\b)?(\s?\-?\s?(20)?(19))?)"
pattern_variant_lineage = r"(\b[A-Z]{1}\.\d{1,4}(\.\d{1,4}){0,4}\b)"


patterns_covid = [{"label": "DISEASE", "pattern": pattern_sars , "id":"covid"},
                  {"label": "DISEASE", "pattern": pattern_covid , "id":"covid"},
                  {"label": "DISEASE", "pattern": pattern_coronavirus, "id":"covid"},
                  {"label": "VAR LINEAGE", "pattern": pattern_variant_lineage, "id":"covid"}]


@Language.component("postprocessing_covid")
def expand_covid_ents(doc):
    pattern_sars = r"((sarsr?|mers)(\s?\-?\s?(covs?))?(\s?\-?\s?2)?(\s?\binfe.{1,10}?\b)?)"
    pattern_covid = r"((covid)(\s?\-?\s?(19))?(\s?\binfe.{1,10}?\b)?)"
    pattern_coronavirus = r"((coronavir.{0,6}?\b)(\s?\bpneumo.{0,8}?\b)?(\s?\binfe.{1,10}?\b)?(\s?\bdiseas.{1,6}?\b)?(\s?\-?\s?(20)?(19))?)"
    pattern_variant_lineage = r"(\b[A-Z]{1}\.\d{1,4}(\.\d{1,4}){0,4}\b)"

    patterns_covid = [{"label": "DISEASE", "pattern": pattern_sars , "id":"covid"},
                      {"label": "DISEASE", "pattern": pattern_covid , "id":"covid"},
                      {"label": "DISEASE", "pattern": pattern_coronavirus, "id":"covid"},
                      {"label": "VAR LINEAGE", "pattern": pattern_variant_lineage, "id":"covid"}]
    
    new_ents = []
    doc_ents = list(doc.ents)
    print(len(doc.ents))
    for pattern in patterns_covid:
        for match in re.finditer(pattern['pattern'], doc.text, re.IGNORECASE):
            start, end = match.span()
            span = doc.char_span(start, end, label=pattern['label'],alignment_mode='expand')
            # This is a Span object or None if match doesn't map to valid token sequence
            if span is not None:
#                 print((span.text, span.label_, span.start, span.end))
                new_ents.append(span)
#                 print("Found match:", span.text)
    ents = doc_ents + new_ents
    filtered_spans = util.filter_spans(ents)
    doc.set_ents(filtered_spans)
    return doc


nlp.add_pipe('postprocessing_covid',before='ner')


print(nlp.pipeline)


doc = nlp(sequence)


len(doc.ents)


print(type(doc.ents))





doc.ents = tuple(list(doc.ents[0:7]))


print([(ent.text, ent.label_) for ent in doc.ents])


entities_html = displacy.render(doc, style="ent",
                                options={"ents": ["DISEASE", "CHEMICAL","GENETIC","GPE","ORG","DATE","CARDINAL"],"colors": colors})





normalized_chems = chemical_service.normalize_chemical_entities(entities.get_chemicals_entities())
normalized_dis = disease_service.normalize_disease_entities(entities.get_diseases_entities())
normalized_gen = genetic_service.normalize_genetic_entities(entities.get_genes_entities())

normalized_ents = {'diseases': normalized_dis, 'chemicals': normalized_chems, 'genetics': normalized_gen}


entities_html = displacy.render(doc, style="ent")


doc = nlp(sequence)


nlp.pipeline


diseases = [f for f in doc.ents if f.label_ == 'DISEASE']


chemicals = [f.text for f in doc.ents if f.label_ == 'CHEMICAL']


print(chemicals)


len(chemicals)


seen = set()
chems = []
for item in chemicals:
    if item not in seen:
        seen.add(item)
        chems.append(item)


print(chems)


len(chems)


import pysolr


solr_engine = pysolr.Solr('http://localhost:8983/solr/drugs', timeout=20)
print(solr_engine.ping())


def unique_terms(entities):
    seen = set()
    ents = []
    for item in entities:
        if item.lower() not in seen:
            if len(item)>1:
                seen.add(item.lower())
                ents.append(item)
    return ents


normalized_chems = []
chems = unique_terms(chemicals)
for chem in chems:
    label = re.sub(r'\W+', ' ', str(chem))
#     print(label)
    solr_query = "term:\""+label+"\"^100 or synonyms:\""+label+"\"^10 or mesh_headings:\""+label+"\"^5"
    results = solr_engine.search(solr_query)
    if len(results) < 1:
#         print('Non results for:', chem)
        chemical = {'text_term' : chem}
    for result in results:
        chemical = {}
        chemical["text_term"] = label
        chemical["found_term"] = result["term"]
        if 'cid' in result:
            chemical['cid'] = result["cid"]
        if 'mesh_id' in result:
            chemical['mesh_id'] = result["mesh_id"]
        if 'chebi_id' in result:
            chemical['chebi_id'] = result["chebi_id"]
        if 'cross_references' in result:
            chemical['cross_references'] = result["cross_references"]
        if 'ATC' in result:
            chemical['ATC'] = result["ATC"]
        if 'ATC_level' in result:
            chemical['ATC_level'] = result["ATC_level"]
        normalized_chems.append(chemical)
        break


print(normalized_chems)


import json
print(json.dumps(normalized_chems))


len(normalized_chems)


with open('normalized_chems.json', 'w') as fout:
    json.dump(normalized_chems, fout)





def paragraph_tokenize(sequence):
    if len(sequence.split('.\n')) < len(sequence.split('. \n')):
        paragraphs = sequence.split('. \n')
    else:
        paragraphs = sequence.split('.\n')
    print('Text split in:', len(paragraphs), 'paragraphs' )
    return paragraphs


entities = class_entities.Entities(sequence, [], [], [])
offset = 0

for i,paragraph in enumerate(paragraphs):
    disease_service.sentence_to_process(paragraph)
    disease_service.set_offset(offset+i)
    disease_results = disease_service.predict()
    print(disease_service.offset)
    offset = len(paragraph)
    entities.append_entities(disease_results)


tokenizer_disease = disease_service.tokenizer

def chunk_text(offset,tokenized_sequence):
    split_index = -1
    for j in range(offset+509,offset-1,-1):
        if tokenized_sequence[j] == '.':
            if tokenized_sequence[j+1][0].isupper():
                split_index = j
                break
    return split_index

print(len(tokenized_sequence))
processing = True
offset=0
tokenized_sequence = tokenizer_disease.tokenize(sequence)
index_list = []
while processing:
    if len(tokenized_sequence[offset:])<510:
        print(len(tokenized_sequence[offset:]))
        processing = True
        break
    split_index = chunk_text(offset, tokenized_sequence)
    print('Split index',split_index)
    print(len(tokenized_sequence[offset:]))
    offset = split_index + 1
    index_list.append(split_index)

chunks = []
for chunk in index_list:
    pattern = tokenized_sequence[chunk-1] + tokenized_sequence[chunk]
    result = re.search(pattern + ' ', sequence)
    print(result)
    if result is None:
        result = re.search(pattern + '\n', sequence)
    elif result is None:
        result = re.search(pattern, sequence)
    chunk_size = result.span()
    chunks.append(chunk_size[1])

start=0
entities = class_entities.Entities(sequence, [], [], [])
for i,chunk in enumerate(chunks):
    chemical_results = []
    chunk_number = i + 1
    if chunk_number<len(chunks):
        print(start)
        disease_service.sentence_to_process(sequence[start:chunk])    
        disease_service.set_offset(start)
        start = chunk
    else:
        print(start)
        print(sequence[start:])
        disease_service.sentence_to_process(sequence[start:])

    disease_results = disease_service.predict()
    

    gene_results = []
    

    entities.append_entities(disease_results)


sequence = "Toluene is a chemical."



chemical_service = chemicalprocessor.ChemicalProcessor('./models/Chemical')

chemical_service.sentence_to_process(sequence)
chemical_results = chemical_service.predict()

entities = class_entities.Entities(sequence, [], chemical_results, [])

print(entities.ents)

entities.remove_non_entities()

entities.correct_boundaries()

print(entities.ents)


for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)


doc.spans


nlp.pipeline


for token in doc:
    print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
            token.shape_, token.is_alpha, token.is_stop)






