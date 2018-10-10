package main

import (
	"log"
	"net"
	"github.com/amoghkashyap/go-coap"
	"go-coap/database/cassandra"
	"time"
)

var (
	//database manager instance
	dbManager    = cassandra.GetInstance()
)
const (
	//Query statements for database transactions
	InserttemperatureQuery     = "INSERT INTO temperature_data (temperature,timestamp) VALUES (?,?)"
)

func handleA(l *net.UDPConn, a *net.UDPAddr, m *coap.Message) *coap.Message {
	//log.Printf("Got message in handleA: path=%q: %#v from %v", m.Path(), m, a)
	log.Printf("Got message in handleA: ")
	temperature := string(m.Payload)

	if m.IsConfirmable() {
		res := &coap.Message{
			Type:      coap.Acknowledgement,
			Code:      coap.Content,
			MessageID: m.MessageID,
			Token:     m.Token,
			Payload:   []byte(" Temperature over the treshold level , cooling initiated"),
		}
		res.SetOption(coap.ContentFormat, coap.TextPlain)


		query := dbManager.Session.Query(InserttemperatureQuery,temperature,time.Now().String())
		if err := query.Exec(); err != nil {
			log.Println("temperature entry failed entry Failed, error: %v", err)
		} else {
			log.Println("temperature entry successful")
			log.Printf("Transmitting to client %#v", res)
			return res
	}}
	return nil
}

func handleB(l *net.UDPConn, a *net.UDPAddr, m *coap.Message) *coap.Message {
	//log.Printf("Got message in handleB: path=%q: %#v from %v", m.Path(), m, a)
	log.Printf("Got message in handleB: ")
	temperature := string(m.Payload)

	if m.IsConfirmable() {
		res := &coap.Message{
			Type:      coap.Acknowledgement,
			Code:      coap.Content,
			MessageID: m.MessageID,
			Token:     m.Token,
			Payload:   []byte("Temperature data updated to cloud"),
		}
		res.SetOption(coap.ContentFormat, coap.TextPlain)
		query := dbManager.Session.Query(InserttemperatureQuery,temperature,time.Now().String())
		if err := query.Exec(); err != nil {
			log.Println("temperature entry failed entry Failed, error: %v", err)
		} else {
			log.Println("temperature entry successful")
			log.Printf("Transmitting to client %#v", res)
			return res
		}}
	return nil
}

func main() {
	mux := coap.NewServeMux()
	mux.Handle("/a", coap.FuncHandler(handleA))
	mux.Handle("/b", coap.FuncHandler(handleB))

	log.Println("COAP service started")
	log.Fatal(coap.ListenAndServe("udp", ":5683", mux))
}
