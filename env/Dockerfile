FROM debian:10

RUN apt-get update
RUN apt-get install --yes python3 postgresql libpq-dev python3-pip git less vim cloc
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1
RUN pip install --upgrade pip
RUN pip install black poetry ipython ipdb flake8
ENV PGUSER="postgres"
COPY pg_hba.conf /etc/postgresql/11/main/pg_hba.conf
LABEL org.opencontainers.image.source https://github.com/arrowresearch/qc0
