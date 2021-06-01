FROM python:3.8
LABEL maintainer1="cbadenes@fi.upm.es"
LABEL maintainer2="alvaro.alonsoc@alumnos.upm.es"
LABEL version="0.1"
LABEL description="Bio-NLP CORD-19 Annotator"

RUN pip install -r ./../requirements.txt

COPY annotate_cord_19.py /app/cord_annotation
COPY ./../models/ /app/
COPY ./../bionlp/ /app/
WORKDIR /app
ENTRYPOINT ["python"]
CMD ["annotate_cord_19.py", "normalize"]
