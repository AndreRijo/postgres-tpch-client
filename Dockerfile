# Debian image with go installed and configured at /go
FROM golang:1.20.4 as base

# Adding go.mod and go.sum of dependencies + of client
#COPY potionDB/potionDB/go.mod potionDB/potionDB/go.sum /go/potionDB/potionDB/
COPY potionDB/crdt/go.mod potionDB/crdt/go.sum /go/potionDB/crdt/
COPY potionDB/shared/go.mod /go/potionDB/shared/
COPY tpch_data_processor/go.mod tpch_data_processor/go.sum /go/tpch_data_processor/
#COPY sqlToKeyValue/go.mod sqlToKeyValue/go.sum /go/sqlToKeyValue/
#COPY goTools/go.mod goTools/go.sum /go/goTools/
COPY postgresTpchGoLib/go.mod postgresTpchGoLib/go.sum /go/postgresTpchGoLib/
COPY postgresTPCHClient/go.mod postgresTPCHClient/go.sum /go/postgresTPCHClient/
RUN cd postgresTPCHClient && go mod download

#Copying local dependencies and client source
#COPY potionDB/potionDB /go/potionDB/potionDB
COPY potionDB/crdt /go/potionDB/crdt
COPY potionDB/shared /go/potionDB/shared
COPY tpch_data_processor/ /go/tpch_data_processor/
#COPY sqlToKeyValue/ /go/sqlToKeyValue/
#COPY goTools/ /go/goTools/
COPY postgresTpchGoLib/ /go/postgresTpchGoLib
COPY postgresTPCHClient/src /go/postgresTPCHClient/src

#Building
RUN ls / && ls /go/
RUN cd postgresTPCHClient/src/main && go build && rm -r /go/bin /go/pkg /go/postgresTpchGoLib /go/potionDB /go/src /go/tpch_data_processor /go/postgresTPCHClient/src/client /go/postgresTPCHClient/src/main/results
RUN ls /go/

COPY postgresTPCHClient/dockerstuff postgresTPCHClient/

#Final image
FROM golang:1.20.4

#Copy needed files from base image
COPY --from=base /go/postgresTPCHClient /go/postgresTPCHClient

#Add config folders late to avoid having to rebuild multiple images
ADD postgresTPCHClient/configs configs/

#Run the client
CMD ["bash", "postgresTPCHClient/start.sh"]