package utils

/*
func main() {
	// Connect to the echo server at localhost:8081
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Println("Error connecting to the server:", err)
		return
	}
	defer conn.Close()

	// Send a message to the server
	message := "Hello, server!"
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println("Error sending message:", err)
		return
	}

	// Receive and print the echoed message from the server
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error receiving message:", err)
		return
	}

	fmt.Printf("Received from server: %s\n", buffer[:n])

} */
