package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

// --- CONFIGURAÇÕES BASE ---
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// --- ESTRUTURAS DE DADOS (JSON) ---
// Como a App e o Servidor vão "falar" um com o outro
type AppMessage struct {
	Type    string `json:"type"`    // Ex: "CONNECT_BRIDGE", "SEND_MESSAGE"
	Network string `json:"network"` // Ex: "WHATSAPP", "TELEGRAM", "MESSENGER", "SMS"
	Payload string `json:"payload"` // Dados extra (ex: número de telefone ou texto da mensagem)
}

type ServerResponse struct {
	Type    string `json:"type"` // Ex: "BRIDGE_STATUS", "INCOMING_MSG"
	Network string `json:"network"`
	Status  string `json:"status"` // Ex: "AWAITING_QR", "AWAITING_SMS", "CONNECTED"
	Data    string `json:"data"`   // Ex: O código QR em Base64, ou o texto da mensagem
}

// Cliente WebSocket
type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
}

var (
	clients    = make(map[*Client]bool)
	register   = make(chan *Client)
	unregister = make(chan *Client)
)

// --- INÍCIO DO SERVIDOR ---
func main() {
	go runHub()

	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("Beeper-Clone Core Server a arrancar na porta " + port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Erro fatal: ", err)
	}
}

func runHub() {
	for {
		select {
		case client := <-register:
			clients[client] = true
			fmt.Println("App ligada! User:", client.ID)
		case client := <-unregister:
			if _, ok := clients[client]; ok {
				delete(clients, client)
				close(client.Send)
				fmt.Println("App desligada! User:", client.ID)
			}
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	userId := r.URL.Query().Get("userId")
	if userId == "" {
		http.Error(w, "userId obrigatório", http.StatusBadRequest)
		return
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Erro WebSocket:", err)
		return
	}

	client := &Client{ID: userId, Conn: ws, Send: make(chan []byte, 256)}
	register <- client

	// Goroutine de Escrita
	go func() {
		defer ws.Close()
		for msg := range client.Send {
			ws.WriteMessage(websocket.TextMessage, msg)
		}
	}()

	// Rotina principal de Leitura
	defer func() {
		unregister <- client
		ws.Close()
	}()

	for {
		_, messageBytes, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var appMsg AppMessage
		err = json.Unmarshal(messageBytes, &appMsg)
		if err != nil {
			fmt.Println("Erro a interpretar JSON da App:", err)
			continue
		}

		// O "CÉREBRO" - Processar o que a App nos pediu
		fmt.Printf("Recebido comando %s para a rede %s\n", appMsg.Type, appMsg.Network)

		if appMsg.Type == "CONNECT_BRIDGE" {
			go handleBridgeConnection(client, appMsg.Network)
		}
	}
}

// --- GESTOR DE BRIDGES (As tuas ligações às Redes) ---
// É aqui que a verdadeira engenharia vai acontecer no futuro.
func handleBridgeConnection(client *Client, network string) {
	// 1. Avisamos a App que estamos a iniciar o processo...
	sendToClient(client, ServerResponse{
		Type:    "BRIDGE_STATUS",
		Network: network,
		Status:  "STARTING",
		Data:    "A iniciar motor de bridge...",
	})

	time.Sleep(2 * time.Second)

	switch network {
	case "WHATSAPP":
		// TODO: Integrar biblioteca "whatsmeow" aqui.
		// Ela vai gerar um QR Code real. Aqui estamos a simular esse envio.
		sendToClient(client, ServerResponse{
			Type:    "BRIDGE_STATUS",
			Network: network,
			Status:  "AWAITING_QR",
			Data:    "Simulação: [IMAGEM DO QR CODE AQUI]",
		})

	case "TELEGRAM":
		// TODO: Integrar biblioteca "gotd" aqui.
		// Precisamos de pedir à app o número de telemóvel primeiro.
		sendToClient(client, ServerResponse{
			Type:    "BRIDGE_STATUS",
			Network: network,
			Status:  "AWAITING_PHONE_NUMBER",
			Data:    "Insira o número de telemóvel",
		})

	case "GOOGLE_MESSAGES", "SMS":
		// TODO: Integrar puppeteer (navegador invisível) para messages.google.com
		sendToClient(client, ServerResponse{
			Type:    "BRIDGE_STATUS",
			Network: network,
			Status:  "AWAITING_QR",
			Data:    "Simulação: [QR CODE DO GOOGLE MESSAGES]",
		})

	case "MESSENGER":
		// TODO: Integrar mqtt/facebook-chat-api
		sendToClient(client, ServerResponse{
			Type:    "BRIDGE_STATUS",
			Network: network,
			Status:  "AWAITING_LOGIN",
			Data:    "Insira credenciais do Meta",
		})
	}

	// Simular que o utilizador leu o QR / Inseriu o Código passado 10 segundos
	time.Sleep(10 * time.Second)
	sendToClient(client, ServerResponse{
		Type:    "BRIDGE_STATUS",
		Network: network,
		Status:  "CONNECTED",
		Data:    "Sincronização de mensagens iniciada com sucesso!",
	})
}

// Função utilitária para converter as respostas em JSON e enviar para a App
func sendToClient(client *Client, resp ServerResponse) {
	jsonBytes, _ := json.Marshal(resp)
	client.Send <- jsonBytes
}
