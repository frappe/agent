web: PYTHONUNBUFFERED=1 FLASK_ENV=development FLASK_DEBUG=1 FLASK_APP=agent.web:application SENTRY_DSN="https://e9b2a2274f2245daab48c6245fb69431@trace.frappe.cloud/17" ./env/bin/flask run -p 25052
redis: redis-server redis.conf
worker_1: PYTHONUNBUFFERED=1 ./repo/wait-for-it.sh redis://127.0.0.1:25025 && ./env/bin/rq worker --url redis://127.0.0.1:25025 high default low --sentry-dsn 'https://e9b2a2274f2245daab48c6245fb69431@trace.frappe.cloud/17'
worker_2: PYTHONUNBUFFERED=1 ./repo/wait-for-it.sh redis://127.0.0.1:25025 && ./env/bin/rq worker --url redis://127.0.0.1:25025 high default low --sentry-dsn 'https://e9b2a2274f2245daab48c6245fb69431@trace.frappe.cloud/17'
