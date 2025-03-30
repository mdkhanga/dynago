package utils

import (
	"fmt"
	"strconv"
	"strings"
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

func ParseHostPort(input string) (string, int32, error) {
	parts := strings.Split(input, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid input format: %s", input)
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number: %s", parts[1])
	}

	return host, int32(port), nil
}
