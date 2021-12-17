web: PYTHONUNBUFFERED=1 FLASK_ENV=development FLASK_DEBUG=1 FLASK_APP=agent.web:application ./env/bin/flask run -p 25052
redis: redis-server redis.conf
worker_1: PYTHONUNBUFFERED=1 ./repo/wait-for-it.sh redis://127.0.0.1:25025 && ./env/bin/rq worker --url redis://127.0.0.1:25025 high default low
worker_2: PYTHONUNBUFFERED=1 ./repo/wait-for-it.sh redis://127.0.0.1:25025 && ./env/bin/rq worker --url redis://127.0.0.1:25025 high default low
