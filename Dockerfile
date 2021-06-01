FROM python:3.8
LABEL maintainer1="cbadenes@fi.upm.es"
LABEL maintainer2="alvaro.alonsoc@alumnos.upm.es"
LABEL version="0.1"
LABEL description="Bio-NLP"

RUN pip install -r requirements.txt

COPY app.py /app/
COPY models/ /app/
COPY bionlp/ /app/
WORKDIR /app
ENTRYPOINT ["python"]
CMD ["app.py"]
