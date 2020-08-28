FROM vitess/bootstrap:common

RUN apt-get update
RUN apt-get install -y sudo curl vim jq mysql-server

RUN mkdir -p /tmp/vitess-src
RUN curl -sL https://github.com/vitessio/vitess/archive/master.tar.gz | tar xz -C /tmp/vitess-src --strip 1
RUN mkdir -p /vt/dist
RUN cp /tmp/vitess-src/docker/local/install_local_dependencies.sh /vt/dist/install_local_dependencies.sh
RUN /vt/dist/install_local_dependencies.sh
RUN echo "source /vt/local/env.sh" >> /etc/bash.bashrc

# Allows some docker builds to disable CGO
ARG CGO_ENABLED=0

# Build and install Vitess in a temporary output directory.
USER vitess

RUN mkdir /vt/local
RUN cp -r /tmp/vitess-src/examples/local/* /vt/local

RUN mkdir -p /vt/src/vitess.io/vitess
# Re-copy sources from working tree.
RUN cp -r /tmp/vitess-src/* /vt/src/vitess.io/vitess

WORKDIR /vt/src/vitess.io/vitess
RUN make install PREFIX=/vt/install

ENV VTROOT /vt/src/vitess.io/vitess
ENV VTDATAROOT /vt/vtdataroot
ENV PATH /var/opt/etcd:$PATH

CMD cd /vt/local && ./101_initial_cluster.sh && tail -f /dev/null
