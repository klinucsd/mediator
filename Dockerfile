FROM postgis/postgis:15-3.4 as builder
RUN apt update
RUN apt install -y postgis
RUN which shp2pgsql

# check shp2pgsql is installed
RUN shp2pgsql

# check raster2pgsql is installed
RUN raster2pgsql

RUN apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev gdal-bin

RUN apt-get install -y tar net-tools curl vim unzip less  libtool git patch make gcc pkg-config libevent-dev

RUN apt-get install -y python3 python3-dev python3-pip

RUN python3 --version

RUN pip3 --version

RUN git --version

# modify pglib_query and install pglast 
RUN git clone https://github.com/lelit/pglast.git && \
    cd /pglast && \  
    git submodule update --init --recursive && \   
    cd /pglast/libpg_query && \ 
    sed -i 's/define NAMEDATALEN 64/define NAMEDATALEN 256/' /pglast/libpg_query/src/postgres/include/pg_config_manual.h  && \ 
    make  && \ 
    cd /  && \ 
    pip3 install ./pglast

COPY requirements.txt /requirements.txt

RUN pip3 install -r requirements.txt

RUN pip3 install arcgis --no-deps

RUN ARCH="" && if [ "$(uname -m)" = "x86_64" ]; then ARCH='amd64'; else ARCH='arm64'; fi && export ${ARCH} && \
    wget https://github.com/jgm/pandoc/releases/download/3.1.6.2/pandoc-3.1.6.2-linux-${ARCH}.tar.gz && \
    tar xvzf ./pandoc-3.1.6.2-linux-${ARCH}.tar.gz --strip-components 1 -C /usr/local && \
    rm -fr pandoc-3.1.6.2-linux-amd64.tar.gz


RUN git clone https://github.com/pgbouncer/pgbouncer.git --branch "stable-1.19" && \
    git clone https://github.com/awslabs/pgbouncer-rr-patch.git && \
    cd pgbouncer-rr-patch && \
    ./install-pgbouncer-rr-patch.sh ../pgbouncer && \
    cd ../pgbouncer && \
    git submodule init && \
    git submodule update && \
    ./autogen.sh && \
    ./configure --prefix=/usr/local --exec-prefix=/usr/bin --bindir=/usr/bin && \
    make && \
    make install && \
    rm -fr ../pgbouncer ../pgbouncer-rr-patch


RUN useradd -ms /bin/bash pgbouncer && \
    chown pgbouncer /home/pgbouncer && \
    chmod a+rwx /home/pgbouncer 


# ENTRYPOINT ["docker-entrypoint.sh"]
# EXPOSE 5432
# CMD ["postgres"]

# Set environment variable
ENV POSTGRES_PASSWORD=pleasechangeittoanythingyoulike

# Copy the entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh

# Make the script executable
RUN chmod +x /docker-entrypoint.sh

# Copy the mediator data loader daemon script
COPY mediator_data_loader_daemon_start.sh /mediator_data_loader_daemon_start.sh
RUN chmod +x /mediator_data_loader_daemon_start.sh

# Copy the entrypoint script
COPY start.sh /start.sh

# Make the script executable
RUN chmod +x /start.sh

# Copy the psql script to initialize the database 
COPY 10_postgis.sh /docker-entrypoint-initdb.d/10_postgis.sh

# Copy the mediator source code
RUN mkdir /home/pgbouncer/src
COPY ./src /home/pgbouncer/src
COPY .env /home/pgbouncer/.env
COPY userlist.txt /home/pgbouncer/userlist.txt
RUN chown -R postgres:postgres /home/pgbouncer

# Set the entry point to the script
ENTRYPOINT ["/docker-entrypoint.sh"]

# EXPOSE 5432 
EXPOSE 6432
CMD ["postgres"]
