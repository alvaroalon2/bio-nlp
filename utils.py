from os import path


def paragraphs(document):
    start = 0
    for token in document:
        if token.is_space and token.text.count("\n") > 1:
            yield document[start:token.i]
            start = token.i
    yield document[start:]


def unique_terms(entities):
    seen = set()
    ents = []
    for item in entities:
        if item.lower() not in seen:
            if len(item) > 1:
                seen.add(item.lower())
                ents.append(item)
    return ents


def check_existant_model(ent):
    flag_ent = False
    if path.isfile('./models/' + ent + '/pytorch_model.bin') and path.isfile(
            './models/' + ent + '/config.json') and path.isfile(
        './models/' + ent + '/tokenizer_config.json') and path.isfile(
        './models/' + ent + '/vocab.txt'):
        flag_ent = True
    return flag_ent

def group_in_dict(normalized_ent):
    normalized_ents = {}
    for k, v in [(key, d[key]) for d in normalized_ent for key in d]:
        if k not in normalized_ents:
            if isinstance(v, list):
                normalized_ents[k] = v
            else:
                normalized_ents[k] = [v]
        else:
            if v not in normalized_ents[k]:
                if isinstance(v, list):
                    normalized_ents[k] += v
                else:
                    normalized_ents[k].append(v)
    return normalized_ents
