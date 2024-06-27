# Debian image with go installed and configured at /go
FROM golang:1.20.4

# Adding go.mod and go.sum of dependencies + of client
COPY potionDB/potionDB/go.mod potionDB/potionDB/go.sum /go/potionDB/potionDB/
COPY potionDB/crdt/go.mod potionDB/crdt/go.sum /go/potionDB/crdt/
COPY potionDB/shared/go.mod /go/potionDB/shared/
COPY tpch_data_processor/go.mod tpch_data_processor/go.sum /go/tpch_data_processor/
COPY sqlToKeyValue/go.mod sqlToKeyValue/go.sum /go/sqlToKeyValue/
COPY postgresTPCHClient/go.mod postgresTPCHClient/go.sum /go/postgresTPCHClient/
RUN cd postgresTPCHClient && go mod download

#Copying local dependencies and client source
COPY potionDB/potionDB /go/potionDB/potionDB
COPY potionDB/crdt /go/potionDB/crdt
COPY potionDB/shared /go/potionDB/shared
COPY tpch_data_processor/ /go/tpch_data_processor/
COPY sqlToKeyValue/ /go/sqlToKeyValue/
COPY postgresTPCHClient/src /go/postgresTPCHClient/src

#Building
RUN cd postgresTPCHClient/src/main && go build

COPY postgresTPCHClient/dockerstuff postgresTPCHClient/

#Add config folders late to avoid having to rebuild multiple images
ADD postgresTPCHClient/configs configs/

#Run the client
CMD ["bash", "postgresTPCHClient/start.sh"]
 
#docker build -f postgresTPCHClient/Dockerfile . -t andrerj/priv:postgresclient
#docker push andrerj/priv:postgresclient
#docker stop -t 0 postgresclient1 ; docker rm postgresclient1 ; docker run -e CONFIG=/go/configs/cluster/0.01SF/ALL/ -e IP=dahu-1:5432 -e USER=arijo -e QUERY_CLIENTS=100 -e QUERY_WAIT=20000 -v "/home/arijo/private/tpch_data/:/go/data/" -v "/home/arijo/private/results/:/go/results/" --name postgresclient1 andrerj/priv:postgresclient