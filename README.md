# movie-api
This project is a part of Search Assist onboarding trainings.

Refer the following assignment for use cases- 
https://docs.google.com/document/d/1zRU_ub60f2v2y4Wk6cLLVqqxmlQYFo6oDxtSCmWEmKY/edit#

This project provides following APIs :-
1. Inserting Json Data into ES, publish appropriate message with job id into Rabbitmq queue - http://127.0.0.1:5000/insert
2. Know the Status of a job  - http://127.0.0.1:5000/status/<job_id>
3. Delete a movie doc by Id - http://127.0.0.1:5000/del/movies/<id>
4. Find number of distinct genres, actors and directors - http://127.0.0.1:5000/unique
5. Get details of a spcific movie based on title - http://127.0.0.1:5000/movies/title/<movie_title>
6. Get all movies by specific actor - http://127.0.0.1:5000/movies/actors/<actor_name>
7. Get all movies in the index - http://127.0.0.1:5000/get/movies

Running the app:

Prerequisites - Install elasticsearch, flask, rabbitmq and celery in our python environment
  
Start rabbitmq and elasticsearch in the background

Run celery tasks, defined in the tasks.py file, in the background :-
>>> celery -A tasks worker -l info

Run rabbitmq queue receiver :-
>>> python receiver.py

Run the main file :-
>>> python main.py

![Movie api](https://user-images.githubusercontent.com/109270412/182840739-93ff7109-272a-4486-a83e-0befd9b6e685.jpg)
