from flask import Flask,request,jsonify
from tasks import *


app = Flask('__name__')
es = Elasticsearch('http://localhost:9200/')

@app.route('/')
def home():
    return '<h1>Head to postman, please!!</h1>'

#API for taking json file and inserting data into ES
@app.route('/insert',methods = ['POST'])
def insert():
    content_type = request.headers.get('Content-Type')

    if (content_type == 'application/json'):
        json = request.json
        response = async_insert.delay(json)
        sender.publish_msg(response.id, "Queued")
        return {"task_id" : str(response.id),
                "task_status": 'Processing'}
    else:
        return 'Content-Type not supported!'


#API to know the status of a task
@app.route('/status/<task_id>',methods = ['get'])
def task_status(task_id):
    task = helper.AsyncResult(task_id, app=helper)

    if task.ready():
        result = task.get()
        return {"task_id" : task.id,
                "task_status": 'SUCCESS',
                "outcome": str(result)}
    else :
        return jsonify(status_code = 202, content={"task_id" : str(task.id),
                "task_status": 'Processing'})


#API for deleting by id
@app.route('/del/<index>/<id>',methods=['post'])
def del_by_id(index,id):
    if (es.exists(index=index, id=id)):
        movie = dict(es.delete(index=index,id = id))
        return movie
    return "No such document present"


#API for count of unique entries in various fields
def get_body(field):
    body = {
        "distinct_name_count": {"cardinality": {"field": str(field)+".keyword"}}
    }
    return body


def unique(field):
    return dict(es.search(index="movies", size=0, aggs=get_body(field)))["aggregations"]["distinct_name_count"]["value"]

@app.route('/unique')
def unique_count():
    body={
            "distinct_name_count": {"cardinality": {"field": "director.keyword"}}
    }

    dir = unique("director")
    genres = unique("genres")
    actors = unique("actors")

    res = {
        "Unique Genres": genres,
        "Unique Directors": dir,
        "Unique Actors": actors
    }
    return res


#API for getting all movies by an actor or by movie title
@app.route('/<index>/<field>/<value>',methods=['get'])
def get_field(index,field,value):
    if es.indices.exists(index=index):
        #check
        if field =="title":
            body = {
                "match_phrase":{
                    field: value
                }
            }
        elif field=="actors":
            body = {
                "multi_match": {
                    "query": value,
                    "fields": [field]
                }
            }
        result = dict(es.search(index=index, query=body))
        print(result)
        return result['hits']
    return "No such document present"

#API for getting all the docs
@app.route('/get/<index>',methods=['get'])
def get_all(index):

    if es.indices.exists(index=index):
        result = es.search(index=index, body={"size":100, "query":{"match_all":{}}})
        res = {}
        for doc in result['hits']['hits']:
            res[doc['_id']] = doc['_source']
        return res
    return "No such document present"



app.run(debug = True)
