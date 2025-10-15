FROM alpine:latest

# Install dependencies
RUN apk add --no-cache git go rust cargo build-base bash

WORKDIR /app

# Clone and build the loadbalancer
RUN git clone https://github.com/bcosso/loadbalancer loadbalancer
WORKDIR /app/loadbalancer
RUN cargo build --release

# Clone and build nimpha
WORKDIR /app
RUN git clone https://github.com/bcosso/nimpha nimpha
WORKDIR /app/nimpha/src
RUN go mod init nimpha
RUN go mod tidy
RUN go build
RUN cp /app/nimpha/src/nimpha /app/nimpha/nimpha
WORKDIR /app/nimpha/
RUN echo "{}" > wal_file.json

# Clone and build the np command line
WORKDIR /app
RUN git clone https://github.com/bcosso/nimpha_commandline np
WORKDIR /app/np
RUN go mod init np
RUN go mod tidy
RUN go build

WORKDIR /app
RUN cp /app/np/np /app/nimpha/np


# Create a script to run loadbalancer and nimpha
WORKDIR /app
RUN cp /app/nimpha/configfile.json /app/configfile.json
RUN cp /app/loadbalancer/target/release/loadbalancer /app/nimpha/lb
RUN echo -e '#!/bin/bash \n cd nimpha && ./nimpha & sleep 5 && cd /app/nimpha && ./lb' > start.sh && chmod +x start.sh
# Run the script
ENTRYPOINT ["/app/start.sh"]
EXPOSE 10000 8000 9090

