package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// UnifiedMessage espelha exatamente a estrutura de dados esperada pela App Android
type UnifiedMessage struct {
	MessageID         string `json:"message_id"`
	Platform          string `json:"platform"`
	PlatformMessageID string `json:"platform_message_id"`
	SenderID          string `json:"sender_id"`
	RecipientID       string `json:"recipient_id"`
	Content           string `json:"content"`
	IsOutgoing        bool   `json:"is_outgoing"`
	Timestamp         string `json:"timestamp"`
	Status            string `json:"status"`
}

// Configuração do Upgrader para aceitar conexões WebSocket da app
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Permite qualquer origem (ideal para desenvolvimento)
	},
}

// Gestor de Conexões (dispositivos Android conectados)
type ClientHub struct {
	clients map[*websocket.Conn]bool
	mu      sync.Mutex
}

var hub = ClientHub{
	clients: make(map[*websocket.Conn]bool),
}

// Registar nova conexão
func (h *ClientHub) add(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = true
}

// Remover conexão fechada
func (h *ClientHub) remove(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, conn)
	conn.Close()
}

// Enviar mensagem em tempo real para a app Android
func (h *ClientHub) Broadcast(msg UnifiedMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		log.Println("Erro ao serializar mensagem:", err)
		return
	}

	for conn := range h.clients {
		err := conn.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.Println("Erro ao enviar para o cliente:", err)
			conn.Close()
			delete(h.clients, conn)
		}
	}
}

// Endpoint do WebSocket: /ws
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Falha no Upgrade WebSocket:", err)
		return
	}
	defer hub.remove(conn)

	hub.add(conn)
	log.Printf("-> [NOVO CLIENTE] App Android conectada! (Total ativas: %d)\n", len(hub.clients))

	// Escuta as mensagens enviadas pela app Android (respostas e novas mensagens)
	for {
		_, messageBytes, err := conn.ReadMessage()
		if err != nil {
			log.Println("App Android desconectou:", err)
			break
		}

		var outgoingMsg UnifiedMessage
		if err := json.Unmarshal(messageBytes, &outgoingMsg); err != nil {
			log.Println("Erro ao ler JSON recebido do Android:", err)
			continue
		}

		log.Printf("-> [MENSAGEM DA APP] Canal: %s | Para: %s | Texto: %s\n",
			outgoingMsg.Platform, outgoingMsg.RecipientID, outgoingMsg.Content)

		dispatchToPlatform(outgoingMsg)
	}
}

// Despachar a mensagem enviada pela app para as redes
func dispatchToPlatform(msg UnifiedMessage) {
	switch msg.Platform {
	case "telegram":
		log.Printf("[TELEGRAM] A enviar para %s: %s\n", msg.RecipientID, msg.Content)
	case "whatsapp":
		log.Printf("[WHATSAPP] A enviar para %s: %s\n", msg.RecipientID, msg.Content)
	default:
		log.Printf("[OUTRO] Plataforma %s: %s\n", msg.Platform, msg.Content)
	}
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)

	// Endpoint simples de teste no browser: http://localhost:8080/health
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Backend Go está operacional!"))
	})

	// Simulação: Envia uma mensagem a cada 30 segundos para demonstrar o tempo real na App
	go func() {
		for {
			time.Sleep(30 * time.Second)
			msg := UnifiedMessage{
				MessageID:         fmt.Sprintf("msg_%d", time.Now().Unix()),
				Platform:          "whatsapp",
				PlatformMessageID: "demo_123",
				SenderID:          "+351912345678",
				RecipientID:       "me",
				Content:           "Olá! Esta é uma mensagem de teste enviada pelo teu servidor Go em tempo real!",
				IsOutgoing:        false,
				Timestamp:         time.Now().UTC().Format(time.RFC3339),
				Status:            "delivered",
			}
			hub.Broadcast(msg)
		}
	}()

	port := ":8080"
	log.Printf("Servidor Go a correr em http://localhost%s/ws\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Erro no servidor:", err)
	}
}
