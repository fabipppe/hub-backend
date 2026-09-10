package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	qrcode "github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type AppMessage struct {
	Type    string `json:"type"`
	Network string `json:"network"`
	Payload string `json:"payload"`
}

type ServerResponse struct {
	Type      string `json:"type"`
	Network   string `json:"network"`
	Status    string `json:"status,omitempty"`
	Connected bool   `json:"connected"`
	Payload   string `json:"payload,omitempty"`
}

type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
}

var (
	clients    = make(map[*Client]bool)
	register   = make(chan *Client)
	unregister = make(chan *Client)
	clientLock sync.Mutex

	// WhatsApp Real
	waClient    *whatsmeow.Client
	waContainer *sqlstore.Container
	waLock      sync.Mutex
)

func main() {
	go runHub()

	// Inicia a base de dados local do WhatsApp
	initWhatsAppStore()

	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("Beeper-Clone Server REAL ativo na porta " + port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Erro fatal: ", err)
	}
}

func initWhatsAppStore() {
	dbLog := waLog.Stdout("Database", "INFO", true)
	// Corrigido: adicionado context.Background() como 1º argumento
	container, err := sqlstore.New(context.Background(), "sqlite", "file:whatsapp.db?_pragma=foreign_keys(1)", dbLog)
	if err != nil {
		log.Println("Erro ao inicializar SQLite WhatsApp:", err)
		return
	}
	waContainer = container
}

func runHub() {
	for {
		select {
		case client := <-register:
			clientLock.Lock()
			clients[client] = true
			clientLock.Unlock()
			fmt.Println("App ligada! User:", client.ID)
		case client := <-unregister:
			clientLock.Lock()
			if _, ok := clients[client]; ok {
				delete(clients, client)
				close(client.Send)
				fmt.Println("App desligada! User:", client.ID)
			}
			clientLock.Unlock()
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

	go func() {
		defer ws.Close()
		for msg := range client.Send {
			ws.WriteMessage(websocket.TextMessage, msg)
		}
	}()

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
			continue
		}

		fmt.Printf("Comando recebido: %s para %s (payload: %s)\n", appMsg.Type, appMsg.Network, appMsg.Payload)

		norm := strings.ToLower(appMsg.Network)

		switch appMsg.Type {
		case "START_BRIDGE", "CONNECT_BRIDGE":
			if norm == "whatsapp" {
				go startRealWhatsAppBridge(client)
			} else if norm == "telegram" {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "Insira o número de telemóvel para receber o código",
					Connected: false,
				})
			} else {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   norm,
					Status:    "Aguardando configuração de autenticação para " + norm,
					Connected: false,
				})
			}

		case "SEND_PHONE":
			if norm == "telegram" {
				fmt.Printf("A pedir código SMS para Telegram número: %s\n", appMsg.Payload)
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "Código de verificação enviado para " + appMsg.Payload,
					Connected: false,
				})
			}

		case "SEND_CODE":
			if norm == "telegram" {
				fmt.Printf("A validar código do Telegram: %s\n", appMsg.Payload)
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "A validar código junto dos servidores do Telegram...",
					Connected: false,
				})
			}
		}
	}
}

func startRealWhatsAppBridge(client *Client) {
	waLock.Lock()
	defer waLock.Unlock()

	if waContainer == nil {
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   "whatsapp",
			Status:    "Erro ao abrir base de dados do WhatsApp no servidor",
			Connected: false,
		})
		return
	}

	// Corrigido: adicionado context.Background() como argumento
	deviceStore, err := waContainer.GetFirstDevice(context.Background())
	if err != nil {
		log.Println("Erro ao obter deviceStore:", err)
		return
	}

	clientLog := waLog.Stdout("Client", "INFO", true)
	if waClient == nil {
		waClient = whatsmeow.NewClient(deviceStore, clientLog)
	}

	// Se já estiver logado anteriormente
	if waClient.Store.ID != nil {
		if !waClient.IsConnected() {
			_ = waClient.Connect()
		}
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   "whatsapp",
			Status:    "Conectado",
			Connected: true,
		})
		return
	}

	// Listener de eventos reais
	waClient.AddEventHandler(func(rawEvt interface{}) {
		switch rawEvt.(type) {
		case *events.Connected:
			fmt.Println("WhatsApp conectado com sucesso!")
			sendToClient(client, ServerResponse{
				Type:      "BRIDGE_STATUS",
				Network:   "whatsapp",
				Status:    "Conectado",
				Connected: true,
			})
		case *events.LoggedOut:
			sendToClient(client, ServerResponse{
				Type:      "BRIDGE_STATUS",
				Network:   "whatsapp",
				Status:    "Sessão terminada",
				Connected: false,
			})
		}
	})

	qrChan, err := waClient.GetQRChannel(context.Background())
	if err != nil {
		log.Println("Erro GetQRChannel:", err)
		return
	}

	err = waClient.Connect()
	if err != nil {
		log.Println("Erro waClient.Connect:", err)
		return
	}

	sendToClient(client, ServerResponse{
		Type:      "BRIDGE_STATUS",
		Network:   "whatsapp",
		Status:    "A gerar QR Code oficial do WhatsApp...",
		Connected: false,
	})

	go func() {
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("QR Code Real recebido do WhatsApp! A enviar imagem para a app...")
				png, err := qrcode.Encode(evt.Code, qrcode.Medium, 256)
				var payload string
				if err == nil {
					payload = "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
				} else {
					payload = evt.Code
				}

				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_QR",
					Network:   "whatsapp",
					Status:    "Lê o código com o WhatsApp no teu telemóvel",
					Payload:   payload,
					Connected: false,
				})
			} else if evt.Event == "success" {
				fmt.Println("QR Code lido com sucesso pelo telemóvel!")
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "whatsapp",
					Status:    "Conectado",
					Connected: true,
				})
				break
			} else if evt.Event == "timeout" {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "whatsapp",
					Status:    "QR Code expirou. Clica em reenviar.",
					Connected: false,
				})
				break
			}
		}
	}()
}

func sendToClient(client *Client, resp ServerResponse) {
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		return
	}
	clientLock.Lock()
	defer clientLock.Unlock()
	if _, ok := clients[client]; ok {
		select {
		case client.Send <- jsonBytes:
		default:
		}
	}
}
