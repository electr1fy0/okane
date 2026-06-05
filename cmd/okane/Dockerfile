FROM golang:1.26
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o server ./cmd/okane

CMD ["./server"]
