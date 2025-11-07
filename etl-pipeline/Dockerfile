FROM python:3.10
ADD . /src
WORKDIR /src

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

RUN pip install --no-cache-dir newrelic

EXPOSE 80

ENTRYPOINT [ "python", "/src/main.py" ]
