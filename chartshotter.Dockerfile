# The dockerfile is currently still WIP and might be broken
FROM golang:1.14 AS build-env
ADD . /src
RUN cd /src && make -B all

# final stage
FROM chromedp/headless-shell:latest
WORKDIR /app
COPY --from=build-env /src/bin /app/
COPY ./config-example.yml /app/config.yml
CMD ["./chatshotter -config config.yml"]