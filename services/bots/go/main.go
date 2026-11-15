// Momentum bot: watches the last 5 trades per symbol.
// When 4 or more are the same side it fires an aggressive FAK order in that direction.
// All symbols run in parallel via goroutines.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "maroof-street/bots/go/pb"
)

const (
	gatewayAddr = "localhost:50051"
	redisAddr   = "localhost:6379"
	apiKey      = "go-bot-key-333"
	window      = 5
	threshold   = 4 // out of window
	qty         = uint32(10)
	pollSecs    = 2
)

var (
	symbols   = []string{"AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"}
	refPrices = map[string]int32{"AAPL": 150, "TSLA": 250, "NVDA": 400, "MSFT": 300, "GOOGL": 170}
	idCounter atomic.Uint64
)

// the engine only has 1,000,000 id slots, so this bot owns the 100k block starting at 300,000 and wraps
func nextID() uint64 { return 300_000 + idCounter.Add(1)%100_000 }

type tradeEntry struct {
	Side string `json:"side"`
}

// signal returns +1 (buy), -1 (sell), or 0 (no signal) based on recent trade momentum.
func signal(rdb *redis.Client, sym string) int {
	raw, err := rdb.LRange(context.Background(), fmt.Sprintf("md:%s:trades", sym), 0, int64(window-1)).Result()
	if err != nil || len(raw) < window {
		return 0
	}
	buys := 0
	for _, r := range raw {
		var t tradeEntry
		if json.Unmarshal([]byte(r), &t) == nil && t.Side == "buy" {
			buys++
		}
	}
	if buys >= threshold {
		return 1
	}
	if (window - buys) >= threshold {
		return -1
	}
	return 0
}

func topOfBook(rdb *redis.Client, sym string) (bid, ask int32) {
	top, err := rdb.HGetAll(context.Background(), fmt.Sprintf("md:%s:top", sym)).Result()
	if err != nil {
		return 0, 0
	}
	fmt.Sscanf(top["best_bid"], "%d", &bid)
	fmt.Sscanf(top["best_ask"], "%d", &ask)
	return
}

func runSymbol(sym string, stub pb.OrderServiceClient, rdb *redis.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		time.Sleep(pollSecs * time.Second)

		sig := signal(rdb, sym)
		if sig == 0 {
			continue
		}

		bid, ask := topOfBook(rdb, sym)
		var price int32
		var dir pb.OrderDirection

		if sig == 1 {
			dir = pb.OrderDirection_SIDE_BUY
			if ask > 0 {
				price = ask + 1
			} else {
				price = refPrices[sym] + 2
			}
		} else {
			dir = pb.OrderDirection_SIDE_SELL
			if bid > 0 {
				price = bid - 1
			} else {
				price = refPrices[sym] - 2
			}
		}

		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", apiKey))
		_, err := stub.SubmitOrder(ctx, &pb.SubmitOrder{
			ApiKey:         apiKey,
			Symbol:         sym,
			Price:          price,
			Quantity:       qty,
			OrderDirection: dir,
			OrderType:      pb.OrderType_ORDER_TYPE_FAK,
			ClientOrderId:  nextID(),
		})
		if err != nil {
			log.Printf("go-bot %s: %v", sym, err)
		} else {
			side := "buy"
			if sig == -1 {
				side = "sell"
			}
			log.Printf("momentum %s %s %d@%d", sym, side, qty, price)
		}
	}
}

func main() {
	conn, err := grpc.NewClient(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	stub := pb.NewOrderServiceClient(conn)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	log.Println("go momentum bot running")

	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go runSymbol(sym, stub, rdb, &wg)
	}
	wg.Wait()
}
