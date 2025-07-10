FROM quay.io/debezium/vitess-lite:v21.0.3

USER root

RUN apt-get update
RUN apt-get install -y sudo curl vim jq

# Install etcd
COPY install_local_dependencies.sh /vt/dist/install_local_dependencies.sh
RUN /vt/dist/install_local_dependencies.sh

# Copy vitess components start-up scripts
COPY local /vt/local

USER vitess
ENV PATH /vt/bin:$PATH
ENV PATH /var/opt/etcd:$PATH
CMD cd /vt/local && ./initial_cluster.sh && tail -f /dev/null
