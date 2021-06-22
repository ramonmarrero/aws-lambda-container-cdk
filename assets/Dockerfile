FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt ./
COPY preprocess.py ./
COPY app.py   ./
COPY abalone.csv ./

RUN pip install -r requirements.txt

CMD ["app.handler"]

