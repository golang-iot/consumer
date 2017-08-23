FROM golang:1.7

RUN go get -u -v github.com/golang-iot/consumer

WORKDIR /go/src/github.com/golang-iot/consumer

RUN mkdir /home/images


COPY main.go main.go

RUN go build  

COPY docker.env .env

RUN ls -l

CMD ["./consumer"]  