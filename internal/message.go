package internal

import "time"

type Message struct {
	Number    float64   `json:"number"`
	CreatedAt time.Time `json:"createdAt"`
}
