FROM python:3.10
ADD . /src
WORKDIR /src

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

ENTRYPOINT [ "python", "/src/main.py" ]
