FROM quay.io/debezium/vitess-lite:v21.0.3

USER root

# Remove MySQL/Percona repos with expired GPG keys, not needed for itests
# This can be removed once we upgrade to an image with the fix here https://github.com/vitessio/vitess/pull/18795
RUN rm -f /etc/apt/sources.list.d/mysql.list /etc/apt/sources.list.d/percona.list

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
