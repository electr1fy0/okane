package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

const ()

type APIHandler struct {
}

func (h *APIHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	select {
	case <-time.After(5 * time.Second):
		w.Write([]byte("sent after 5 seconds"))
	case <-r.Context().Done():
		http.Error(w, "request cancelled", http.StatusRequestTimeout)
		return
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	r := http.NewServeMux()
	h := &APIHandler{}
	r.HandleFunc("/payments", h.CreatePayment)
	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Println("starting server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalln("server crashed:", err)
		}
	}()

	<-ctx.Done()
	stop()
	shutDownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := server.Shutdown(shutDownCtx)
	if err != nil {
		log.Println("failed to wait for ongoing reqs to finish")
	}

	log.Println("server shut down")

}
