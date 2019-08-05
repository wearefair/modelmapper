FROM 889883130442.dkr.ecr.us-west-2.amazonaws.com/fair-images:python-3.6.5-2

ARG GITHUB_OAUTH_TOKEN

RUN apt-get update && apt-get install -y --no-install-recommends \ 
	libpq-dev \
	libffi-dev \
	gcc \
	ssh

RUN mkdir /app
ADD . /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt --process-dependency-links

EXPOSE 3000
ENV FLASK_CONFIG production

CMD gunicorn --config gunicorn_config.py wsgi:app
