import pysolr
import time


solr = pysolr.Solr('http://localhost:8983/solr/drugs', timeout=10)
print(solr.ping())


with open('data/Chemical/drugs.json', 'r') as fout:
    drugs = json.loads(fout.read())
    print(len(drugs))


solr.add(drugs[0:50000])
print(solr.commit())
print('50000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[50000:100000])
print(solr.commit())
print('100000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[100000:150000])
print(solr.commit())
print('150000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[150000:200000])
print(solr.commit())
print('200000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[200000:250000])
print(solr.commit())
print('250000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[250000:300000])
print(solr.commit())
print('300000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[300000:350000])
print(solr.commit())
print('350000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[350000:400000])
print(solr.commit())
print('400000 files added')
print('-----------------')
time.sleep(1)
solr.add(drugs[400000:])
print(solr.commit())
print(str(len(drugs)) + ' files added')
print('-----------------')


solr = pysolr.Solr('http://localhost:8983/solr/diseases', timeout=10)
print(solr.ping())


with open('data/Disease/diseases.json', 'r') as fout:
    diseases = json.loads(fout.read())
    print(len(diseases))


solr.add(diseases)
print(solr.commit())
print(len(diseases),'files added')














term = 'carnitine'


results = solr.search('term:'+ term +'^1' + 'or synonyms:' + term,**{'fl':'*,score'})


print("Saw {0} result(s).".format(len(results)))


for result in results:
    print("The title is '{0}'.".format(result['term']))
    print("The score is '{0}'.".format(result['score']))
    print("-------------------------------------------")


result['score']






