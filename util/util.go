package util

import (
    "hash/fnv"

    "github.com/VincentFF/simpleredis/logger"
)

// HashKey hash a string to an int value using fnv32 algorithm
func HashKey(key string) (int, error) {
    fnv32 := fnv.New32()
    _, err := fnv32.Write([]byte(key))
    if err != nil {
        logger.Error("HashKey(%s) error: %v", key, err)
        return -1, err
    }
    return int(fnv32.Sum32()), nil
}
