import flask
from flask import request, jsonify, abort
from flask import render_template
from flask_cors import CORS, cross_origin
from flaskext.markdown import Markdown

from NER_processing import nlp, disease_service, chemical_service, genetic_service

from spacy import displacy

colors = {"DISEASE": "linear-gradient(90deg, #aa9cfc, #fc9ce7)",
          "CHEMICAL": "linear-gradient(90deg, #ffa17f, #3575ad)",
          "GENETIC": "linear-gradient(90deg, #c21500, #ffc500)"}

# article_service = articleprocessor.ArticleProcessor()
# paragraph_service = paragraphprocessor.ParagraphProcessor()

app = flask.Flask(__name__)
Markdown(app)
CORS(app, support_credentials=True, resources={r"/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


#     return '''<h1>librAIry Bio-NLP</h1>
# <p>A prototype API for NLP tasks in biomedical domain.</p>'''


@app.route('/bio-nlp', methods=['GET'])
def biohome():
    return '''<h1>librAIry Bio-NLP</h1>
<p>A prototype API for NLP tasks in biomedical domain:
    <ul>
      <li>Drugs</li>
    </ul>
</p>'''


# def _get_drugs(request):
#     drugs = []
#     if (request.args.get('keywords')):
#         keywords = request.args.get('keywords')
#         for keyword in keywords.split("+"):
#             drugs.append(drug_service.get_drug_by_keyword(keyword))
#     return drugs


##################################################################################################
######
######          Bio NLP
######
##################################################################################################


@app.route('/bio-nlp/entities', methods=['POST'])
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def post_search_entities():
    if not request.json or not 'text' in request.json:
        abort(400)
    sequence = request.json['text']
    doc = nlp(sequence)

    entities_html = displacy.render(doc, style="ent",
                                    options={"ents": ["DISEASE", "CHEMICAL", "GENETIC", "DATE","GPE", "COVID LINEAGE"],
                                             "colors": colors})

    chemicals = [f.text for f in doc.ents if f.label_ == 'CHEMICAL']
    diseases = [f.text for f in doc.ents if f.label_ == 'DISEASE']
    genetics = [f.text for f in doc.ents if f.label_ == 'GENETIC']

    normalized_chems = chemical_service.normalize_chemical_entities(chemicals)
    normalized_dis = disease_service.normalize_disease_entities(diseases)
    normalized_gen = genetic_service.normalize_genetic_entities(genetics)
    normalized_covid = genetic_service.normalize_covid_entities(genetics+chemicals)

    normalized_ents = {'diseases': normalized_dis, 'chemicals': normalized_chems, 'genetics': normalized_gen,
                       'covid': normalized_covid}

    return jsonify(html=entities_html, entities=normalized_ents)


# ##################################################################################################
# ######
# ######          Bio API
# ######
# ##################################################################################################
#
# def get_query_param(request):
#     keywords = ""
#     if ('keywords' in request.args):
#         keywords = request.args['keywords']
#
#     drugs_filter = []
#     diseases_filter = []
#     text_filter = []
#
#     for keyword in keywords.split(","):
#         if (keyword == ""):
#             print("no params")
#             break
#
#         for kw_as_drug in  drug_service.find_drugs(keyword):
#             if (('level' in kw_as_drug) and ('code' in kw_as_drug)):
#                 drugs_filter.append("bionlp_drugs_C"+str(kw_as_drug['level'])+":"+kw_as_drug['code'])
#                 break
#
#         for kw_as_disease in  disease_service.find_diseases(keyword):
#             if (('level' in kw_as_disease) and ('code' in kw_as_disease)):
#                 diseases_filter.append("bionlp_diseases_C"+str(kw_as_disease['level'])+":"+kw_as_disease['code'])
#                 break
#
#         text_filter.append("text_t:\""+keyword+"\"")
#
#     query_filter = []
#     if (len(drugs_filter) > 0 ):
#         query_filter.append("(" + " AND ".join(drugs_filter) + ")")
#     if (len(diseases_filter) > 0 ):
#         query_filter.append("(" + " AND ".join(diseases_filter)  + ")")
#     if (len(text_filter) > 0 ):
#         query_filter.append("(" + " AND ".join(text_filter)  + ")")
#     query = "*:*"
#     if (len(query_filter) >0 ):
#         query = " OR ".join(query_filter)
#     return query
#
# def get_size_param(request):
#     size = 10
#     if ('size' in request.args):
#         size = request.args['size']
#     return int(size)
#
# def get_level_param(request):
#     level=-1
#     if ('level' in request.args):
#         level = request.args['level']
#     return int(level)
#
#
# @app.route('/bio-api/drugs', methods=['GET'])
# def get_drugs():
#     size = get_size_param(request)
#     level= get_level_param(request)
#     query = get_query_param(request)
#     drugs = drug_service.get_drugs(query,size,level)
#     return jsonify(drugs)
#
# @app.route('/bio-api/replacements', methods=['GET'])
# def get_interactions():
#     size = get_size_param(request)
#     level= get_level_param(request)
#
#     keywords = ""
#     if ('keywords' in request.args):
#         keywords = request.args['keywords'].split(",")[0]
#
#     interactions = []
#     if (len(keywords) > 0):
#         drugs = drug_service.find_drugs(keywords)
#         if (len(drugs) > 0):
#             drug = drugs[0]
#             interactions.extend( drug_service.get_related_drugs(drug['code'],size,level))
#     return jsonify(interactions)
#
#
# @app.route('/bio-api/diseases', methods=['GET'])
# def get_diseases():
#     size = get_size_param(request)
#     level= get_level_param(request)
#     query = get_query_param(request)
#     diseases = disease_service.get_diseases(query,size,level)
#     return jsonify(diseases)
#
# @app.route('/bio-api/texts', methods=['GET'])
# def get_paragraphs():
#     size = get_size_param(request)
#     query = get_query_param(request)
#     paragraphs = paragraph_service.get_paragraphs(query,size)
#     return jsonify(paragraphs)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
