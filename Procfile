web: FLASK_ENV=development FLASK_APP=agent.web:application ./env/bin/flask run -p 25052
redis: redis-server --port 25025
worker_1: ./env/bin/rq worker --url redis://127.0.0.1:25025
worker_2: ./env/bin/rq worker --url redis://127.0.0.1:25025
