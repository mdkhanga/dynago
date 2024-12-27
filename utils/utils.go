package utils

import (
	"strconv"
)

func StringToInt32(s string) (int32, error) {

	value, err := strconv.ParseInt(s, 10, 32) // Base 10, 32-bit size
	if err != nil {
		return 0, err
	}

	// Cast the int64 to int32
	int32Value := int32(value)

	return int32Value, nil

}
