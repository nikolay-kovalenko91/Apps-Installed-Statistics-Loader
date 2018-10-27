FROM golang:alpine as builder

RUN apk update && apk add git

RUN mkdir -p /app/src
COPY . /app/src
WORKDIR /app/src

RUN go get -d -v

RUN GOOS=linux GOARCH=386 go build -o /app/bin

FROM scratch

COPY --from=builder /app/bin /go/bin/app

ENTRYPOINT [  \
    "/go/bin/app",  \
    "-idfa=idfa_db:6379",  \
    "-gaid=gaid_db:6379",  \
    "-adid=adid_db:6379",  \
    "-dvid=dvid_db:6379",  \
    "-pattern=/app_data/input_files/*.tsv.gz"  \
]

