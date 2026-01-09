package main

import (
	"collector/internal/app"
	"context"
	"log"
)

func main() {
	w, err := app.NewWorker()
	if err != nil {
		log.Fatal("Ошибка запуска воркера:", err)
	}
	if err = w.Run(context.Background()); err != nil {
		log.Fatal("Ошибка запуска воркера:", err)
	}
}
