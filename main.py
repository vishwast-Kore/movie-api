from flask import Flask,request,jsonify
from tasks import *
import json
app = Flask('__name__')
es = Elasticsearch('http://localhost:9200/')

'''
Improvements needed to be done today:

Done - Add exception handling
Done - Get info from user as request payload
Done - Return statements should be an object 
Done - Open the file from local directory 
Make a workflow diagram

'''

@app.route('/')
def home():
    return '<h1>Head to postman, please!!</h1>'

#API for taking json file and inserting data into ES
@app.route('/insert',methods = ['POST'])
def insert():
    #content_type = request.headers.get('Content-Type')
    try:
        with open('Movies_DB.json') as f:
            data = json.load(f)
    except FileNotFoundError:
        return jsonify(status_code=404, content={"Message": "File not found"})
    #print(data)
    if data:
        try:
            task = async_insert.delay(data)
            print (task)
            return jsonify(status_code = 202, content={"task_id" : str(task.id),
                "task_status": 'Processing'})
        except:
            return jsonify(status_code=501, content={"Internal Server Error": "Could not load data into the server"})
    else:
        return jsonify(status_code = 404, content={"Message":"File not found"})

#API to know the status of a task
@app.route('/status',methods = ['get'])
def task_status():

    task_id = request.form.get('id')

    task = helper.AsyncResult(task_id, app=helper)
    if task.ready():
        result = task.get()
        return jsonify(status_code=201, content={"task_id": str(task.id),
                                                 "task_status": 'Completed'})
    else:
        return jsonify(status_code=202, content={"task_id": str(task.id),
                                                 "task_status": 'Processing'})


#API for deleting by id
@app.route('/delete',methods=['post'])
def del_by_id():
    index = request.form.get('index')
    id = request.form.get('id')
    if es.exists(index=index, id=id):
        movie = dict(es.delete(index=index,id = id))
        return jsonify(status_code=201, content={"Message":"Record deleted successfully","Deleted Entry":movie})
    return jsonify(status_code = 404, content={"Message":"No document found"})


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
    index = request.form.get('index')
    if es.indices.exists(index=index):
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
        return jsonify(status_code=201, content=res)
    else :
        return jsonify(status_code=404, content={"Message":"No such index found"})


#API for getting all movies by an actor or by movie title
@app.route('/find',methods=['get','post'])
def get_field():
    index = request.form.get('index')
    field = request.form.get('field')
    value = request.form.get('value')

    if es.indices.exists(index=index):
        #check
        if field =="title" or field=="actors":
            body = {
                "match_phrase":{
                    field: value
                }
            }
        else :
            return jsonify(status_code=404, content={"Message": "No such field found"})
        result = dict(es.search(index=index, query=body))

        return jsonify(status_code=201, content=result['hits'])
    return jsonify(status_code=404, content={"Message": "No such index found"})


#API for getting all the docs
@app.route('/get/<index>',methods=['get'])
def get_all(index):

    if es.indices.exists(index=index):
        result = es.search(index=index, body={"size":100, "query":{"match_all":{}}})
        res = {}
        for doc in result['hits']['hits']:
            res[doc['_id']] = doc['_source']
        return jsonify(status_code=201, content=res)
    return jsonify(status_code=404, content={"Message": "No such index found"})



app.run(debug = True)
