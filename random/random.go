package random

import (
    "math/rand"
    "time"
)

const (
    allWord   = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    upperWord = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    lowerWord = "abcdefghijklmnopqrstuvwxyz"
)

// RandomN 根据最大值生成随机整数
func RandomN(n int) int {
    rand.Seed(time.Now().UnixNano())
    return rand.Intn(n)
}

// GetRandomBytes 生成随机字符串bytes
func GetRandomBytes(len int) []byte {
    s := []byte(allWord)
    result := make([]byte, len)
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    for i := 0; i < len; i++ {
        result[i] = s[r.Intn(62)]
    }

    return result
}

// GetCapitalRandom 生成大写随机字符串bytes
func GetCapitalRandom(len int) []byte {
    s := []byte(upperWord)
    result := make([]byte, len)
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    for i := 0; i < len; i++ {
        result[i] = s[r.Intn(26)]
    }

    return result
}

// GetLowerRandom 生成小写随机bytes
func GetLowerRandom(len int) []byte {
    s := []byte(lowerWord)
    result := make([]byte, len)
    r := rand.New(rand.NewSource(time.Now().UnixNano()))
    for i := 0; i < len; i++ {
        result[i] = s[r.Intn(26)]
    }

    return result
}
