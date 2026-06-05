FROM golang:1.26


WORKDIR /mockprovider

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o provider ./cmd/mockprovider

CMD ["./provider"]
