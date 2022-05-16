package utils

import (
	"github.com/oklog/ulid/v2"
	"math/rand"
	"strings"
	"time"
)

func GenerateRandomString() string {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	str := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	return strings.ToLower(str)
}
