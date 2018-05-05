FROM golang:1.9.2

WORKDIR /go/src/go_rtb_consumer
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["go_rtb_consumer"]
