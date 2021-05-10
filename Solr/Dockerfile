FROM solr

MAINTAINER  Alvaro Alonso "alvaro.alonsoc@alumnos.upm.es"

EXPOSE 8983

COPY drugs /opt/solr/server/solr/configsets/drugs
#COPY diseases /opt/solr/server/solr/configsets/diseases
#COPY genetic /opt/solr/server/solr/configsets/genetic
#COPY covid /opt/solr/server/solr/configsets/covid

USER root
RUN chown -R solr:solr /opt/solr/server/solr/configsets/drugs
#RUN chown -R solr:solr /opt/solr/server/solr/configsets/diseases
#RUN chown -R solr:solr /opt/solr/server/solr/configsets/genetic
#RUN chown -R solr:solr /opt/solr/server/solr/configsets/covid
#COPY --chown=solr:solr ./create_cores.sh /docker-entrypoint-initdb.d/

USER solr

#RUN /opt/solr/bin/solr start && \
#    /opt/solr/bin/solr create_core -c drugs -d drugs && \
#    /opt/solr/bin/solr stop

# RUN /opt/docker-solr/scripts/precreate-core  drugs /opt/solr/server/solr/configsets/drugs

#CMD bash -c "precreate-core core1; precreate-core core2; exec solr -f"
