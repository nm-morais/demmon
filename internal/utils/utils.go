package utils

import (
	"crypto/rand"
	"math"
	"math/big"
)

func GetRandInt(max int) int {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}

func GetRandFloat64() float64 {
	n, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(err)
	}
	return float64(n.Int64()) / float64(math.MaxInt64)
}
