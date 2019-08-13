FROM tiangolo/uvicorn-gunicorn-starlette:python3.7

ENV PORT=8080

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app
