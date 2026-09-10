package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// --- CONFIGURAÇÕES BASE ---
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// --- ESTRUTURAS DE DADOS (JSON) ---
type AppMessage struct {
	Type    string `json:"type"`    // Ex: "START_BRIDGE", "CONNECT_BRIDGE", "SEND_MESSAGE"
	Network string `json:"network"` // Ex: "whatsapp", "telegram", "messenger", "messages"
	Payload string `json:"payload"` // Dados extra
}

type ServerResponse struct {
	Type      string `json:"type"`                // "BRIDGE_STATUS", "BRIDGE_QR", "BRIDGE_CONNECTED"
	Network   string `json:"network"`             // "whatsapp", "telegram", etc.
	Status    string `json:"status,omitempty"`    // "A gerar QR Code...", "CONNECTED"
	Connected bool   `json:"connected,omitempty"` // true / false
	Payload   string `json:"payload,omitempty"`   // QR Code ou mensagem
	Data      string `json:"data,omitempty"`      // Texto extra para retrocompatibilidade
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

		// Aceita tanto START_BRIDGE como CONNECT_BRIDGE
		if appMsg.Type == "START_BRIDGE" || appMsg.Type == "CONNECT_BRIDGE" {
			go handleBridgeConnection(client, appMsg.Network)
		}
	}
}

// --- GESTOR DE BRIDGES ---
func handleBridgeConnection(client *Client, network string) {
	normNetwork := strings.ToLower(network)

	// 1. Avisar imediatamente a App que o processo iniciou
	sendToClient(client, ServerResponse{
		Type:      "BRIDGE_STATUS",
		Network:   normNetwork,
		Status:    "A iniciar motor da bridge...",
		Connected: false,
	})

	time.Sleep(1 * time.Second)

	switch normNetwork {
	case "whatsapp":
		// Envia o QR Code para a app
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_QR",
			Network:   normNetwork,
			Status:    "QR Code pronto para scan",
			Payload:   "2@SIMULACAO_QR_CODE_WHATSMEOW_TESTE",
			Connected: false,
		})

	case "telegram":
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   normNetwork,
			Status:    "Aguardando autenticação por telefone/código...",
			Connected: false,
		})

	case "google_messages", "sms", "messages":
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_QR",
			Network:   normNetwork,
			Status:    "QR Code Google Mensagens pronto",
			Payload:   "DEVICE_PAIRING_CODE_SIMULADO",
			Connected: false,
		})

	case "messenger":
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   normNetwork,
			Status:    "Aguardando credenciais do Meta...",
			Connected: false,
		})
	}

	// Simula a confirmação da ligação após 10 segundos
	time.Sleep(10 * time.Second)
	sendToClient(client, ServerResponse{
		Type:      "BRIDGE_STATUS",
		Network:   normNetwork,
		Status:    "Conectado",
		Connected: true,
	})
}

// Função utilitária para converter as respostas em JSON e enviar para a App
func sendToClient(client *Client, resp ServerResponse) {
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		fmt.Println("Erro no Marshal:", err)
		return
	}
	client.Send <- jsonBytes
}
