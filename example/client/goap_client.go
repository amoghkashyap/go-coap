package main

import (
	"log"
	"github.com/amoghkashyap/go-coap"
	"strconv"
)

var(
	temperature = "26"
	path = ""
)

func main() {

	//Read temperature here
	temperatureInt,_ := strconv.Atoi(temperature)
	req := coap.Message{
		Type:      coap.Confirmable,
		Code:      coap.GET,
		MessageID: 12345,
		Payload:   []byte(temperature),
	}

	if temperatureInt >= 23{
	path = "/a"
	} else{
	path = "/b"
	}

	req.SetOption(coap.ETag, "weetag")
	req.SetOption(coap.MaxAge, 3)
	req.SetPathString(path)

	c, err := coap.Dial("udp", "localhost:5683")
	if err != nil {
		log.Fatalf("Error dialing: %v", err)
	}

	rv, err := c.Send(req)
	if err != nil {
		log.Fatalf("Error sending request: %v", err)
	}

	if rv != nil {
		log.Printf("Response payload: %s", rv.Payload)
	}

}
