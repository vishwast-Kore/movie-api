import celery
from celery import Celery
from celery import current_task
from elasticsearch import Elasticsearch
import config
import time
import sender

helper = Celery('tasks', backend=config.credentials["BACKEND"],broker=config.credentials["BROKER_URL"])
es = Elasticsearch(config.credentials["ES_SERVER"])

@helper.task()
def async_insert(json):
    dict_keys = json.keys()
    i = 1
    try:
        current = current_task.request
    except:
        sender.publish_msg("Task creation failed")
        return "Task creation failed"

    sender.publish_msg(current.id, "Queued")
    time.sleep(0.5)
    sender.publish_msg(current.id,"In process")


    try:
        for index in dict_keys:
            for doc in json[index]:
                if 'id' in doc:
                    del doc['id']
                record = dict(es.index(index=index, id=i, document=doc))
                time.sleep(0.5)
                i += 1

    #Converitng actors field from string -> list
        body = {
            "processors" : [
            {
                "split": {
                    "field": "actors",
                    "separator": ", "
                }
            }
        ]
        }
        print("Here")
        time.sleep((0.1))
        es.ingest.put_pipeline(id="splitter", body=body)
        es.update_by_query(index=index, pipeline="splitter")
    except:
        sender.publish_msg(current.id, "Failure")
        return "Records not added"

    sender.publish_msg(current.id, "Completed")
    return "Records added"


#Command to start celery worker in the background
#celery -A tasks worker -l info
