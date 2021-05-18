
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
            if len(item)>1:
                seen.add(item.lower())
                ents.append(item)
    return ents
