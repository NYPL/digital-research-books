FROM python:3.10
ADD . /src
WORKDIR /src

RUN pip install --upgrade pip wheel setuptools

RUN pip install -r requirements.txt

RUN pip install --no-cache-dir newrelic

RUN pip install elasticsearch>8.0.0

EXPOSE 80

ENTRYPOINT [ "python", "/src/main.py" ]
