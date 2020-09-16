FROM fedora:32
RUN dnf install -y gcc g++ git cmake make java-11-openjdk-devel zlib-devel tbb-devel telnet python3-flask httpd python3-mod_wsgi pybind11-devel python3-devel

WORKDIR /

RUN mkdir -p /data/PDBe_clone_binary /data/PDBe_clone

RUN touch /data/pivots

RUN mkdir -p /var/local/ProteinSearch/computations

RUN chown -R apache:apache /var/local/ProteinSearch

RUN mkdir -p /usr/local/www/ProteinSearch

COPY docker/ProteinSearch.conf /etc/httpd/conf.d/

WORKDIR /usr/src

# set --build-arg REBUILD=$(date +%s) to rebuild image from here
ARG REBUILD=unknown

RUN git clone https://github.com/krab1k/gesamt_distance

WORKDIR /usr/src/gesamt_distance

RUN mkdir build

WORKDIR /usr/src/gesamt_distance/build

RUN cmake ..

RUN make -j7

RUN make install

COPY app/ /usr/local/www/ProteinSearch/

COPY docker/config.py /usr/local/www/ProteinSearch/

ENTRYPOINT ["/usr/sbin/httpd", "-D", "FOREGROUND"]

EXPOSE 8888
