package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	qrcode "github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type AppMessage struct {
	Type      string `json:"type"`
	Network   string `json:"network"`
	Payload   string `json:"payload"`
	Recipient string `json:"recipient,omitempty"`
}

type ServerResponse struct {
	Type      string `json:"type"`
	Network   string `json:"network"`
	Status    string `json:"status,omitempty"`
	Connected bool   `json:"connected"`
	Payload   string `json:"payload,omitempty"`
	Sender    string `json:"sender,omitempty"`
	Message   string `json:"message,omitempty"`
	ChatName  string `json:"chatName,omitempty"`
}

type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
}

var (
	clients      = make(map[*Client]bool)
	register     = make(chan *Client)
	unregister   = make(chan *Client)
	clientLock   sync.Mutex
	waClient     *whatsmeow.Client
	waContainer  *sqlstore.Container
	waLock       sync.Mutex
	activeClient *Client
)

func main() {
	go runHub()
	initWhatsAppStore()

	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("Beeper-Clone Server com Histórico Ativo na porta " + port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Erro fatal: ", err)
	}
}

func initWhatsAppStore() {
	dbLog := waLog.Stdout("Database", "INFO", true)
	connStr := "file:whatsapp.db?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_busy_timeout=5000"
	container, err := sqlstore.New(context.Background(), "sqlite", connStr, dbLog)
	if err != nil {
		log.Println("Erro SQLite WhatsApp:", err)
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
			activeClient = client
			clientLock.Unlock()
			fmt.Println("App ligada! User:", client.ID)
		case client := <-unregister:
			clientLock.Lock()
			if _, ok := clients[client]; ok {
				delete(clients, client)
				if activeClient == client {
					activeClient = nil
				}
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
		userId = "default_user"
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Erro WebSocket:", err)
		return
	}

	client := &Client{ID: userId, Conn: ws, Send: make(chan []byte, 256)}
	register <- client

	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			clientLock.Lock()
			if _, ok := clients[client]; !ok {
				clientLock.Unlock()
				return
			}
			clientLock.Unlock()
			_ = ws.WriteMessage(websocket.PingMessage, nil)
		}
	}()

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

		norm := strings.ToLower(appMsg.Network)

		switch appMsg.Type {
		case "PING":
			sendToClient(client, ServerResponse{Type: "PONG", Network: norm, Connected: true})

		case "START_BRIDGE", "CONNECT_BRIDGE":
			if norm == "whatsapp" {
				go startRealWhatsAppBridge(client)
			} else {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   norm,
					Status:    "Rede " + norm + " aguarda configuração",
					Connected: false,
				})
			}

		case "PAIR_PHONE":
			if norm == "whatsapp" {
				go handlePairPhoneWhatsApp(client, appMsg.Payload)
			}

		case "SEND_MESSAGE":
			if norm == "whatsapp" && waClient != nil && waClient.IsConnected() {
				recipient := appMsg.Recipient
				if !strings.HasSuffix(recipient, "@s.whatsapp.net") && !strings.HasSuffix(recipient, "@g.us") {
					recipient += "@s.whatsapp.net"
				}
				jid, err := types.ParseJID(recipient)
				if err == nil {
					_, _ = waClient.SendMessage(context.Background(), jid, &waE2E.Message{
						Conversation: &appMsg.Payload,
					})
				}
			}
		}
	}
}

func ensureClientInitialized() error {
	if waClient != nil {
		return nil
	}
	if waContainer == nil {
		return fmt.Errorf("container is nil")
	}
	deviceStore, err := waContainer.GetFirstDevice(context.Background())
	if err != nil {
		return err
	}
	clientLog := waLog.Stdout("Client", "INFO", true)
	waClient = whatsmeow.NewClient(deviceStore, clientLog)
	setupEventHandlers()
	return nil
}

func handlePairPhoneWhatsApp(client *Client, phone string) {
	waLock.Lock()
	defer waLock.Unlock()

	err := ensureClientInitialized()
	if err != nil {
		return
	}

	if !waClient.IsConnected() {
		_ = waClient.Connect()
	}

	cleanPhone := strings.ReplaceAll(phone, "+", "")
	cleanPhone = strings.ReplaceAll(cleanPhone, " ", "")
	cleanPhone = strings.ReplaceAll(cleanPhone, "-", "")

	code, err := waClient.PairPhone(context.Background(), cleanPhone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   "whatsapp",
			Status:    "Erro: " + err.Error(),
			Connected: false,
		})
		return
	}

	sendToClient(client, ServerResponse{
		Type:      "BRIDGE_QR",
		Network:   "whatsapp",
		Status:    "Insere este código no WhatsApp",
		Payload:   code,
		Connected: false,
	})
}

func startRealWhatsAppBridge(client *Client) {
	waLock.Lock()
	defer waLock.Unlock()

	err := ensureClientInitialized()
	if err != nil {
		return
	}

	if waClient.Store.ID != nil {
		if !waClient.IsConnected() {
			_ = waClient.Connect()
		}
		sendToClient(client, ServerResponse{
			Type:      "BRIDGE_STATUS",
			Network:   "whatsapp",
			Status:    "Conectado e Sincronizado",
			Connected: true,
		})
		return
	}

	qrChan, err := waClient.GetQRChannel(context.Background())
	if err != nil {
		return
	}

	err = waClient.Connect()
	if err != nil {
		return
	}

	sendToClient(client, ServerResponse{
		Type:      "BRIDGE_STATUS",
		Network:   "whatsapp",
		Status:    "A gerar QR Code...",
		Connected: false,
	})

	go func() {
		for evt := range qrChan {
			if evt.Event == "code" {
				png, err := qrcode.Encode(evt.Code, qrcode.Medium, 256)
				var payload string
				if err == nil {
					payload = "data:image/png;base64,+base64.StdEncoding.EncodeToString(png)"
				} else {
					payload = evt.Code
				}

				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_QR",
					Network:   "whatsapp",
					Status:    "Lê o QR Code",
					Payload:   payload,
					Connected: false,
				})
			} else if evt.Event == "success" {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "whatsapp",
					Status:    "Conectado",
					Connected: true,
				})
				break
			}
		}
	}()
}

func setupEventHandlers() {
	if waClient == nil {
		return
	}
	waClient.AddEventHandler(func(rawEvt interface{}) {
		switch evt := rawEvt.(type) {
		case *events.Connected:
			if activeClient != nil {
				sendToClient(activeClient, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "whatsapp",
					Status:    "Conectado e Sincronizado",
					Connected: true,
				})
			}
		case *events.HistorySync:
			if activeClient != nil && evt.Data != nil {
				fmt.Printf("Histórico sincronizado recebido: %d conversas\n", len(evt.Data.GetConversations()))
				for _, conv := range evt.Data.GetConversations() {
					chatName := conv.GetID()
					if conv.GetName() != "" {
						chatName = conv.GetName()
					}

					sendToClient(activeClient, ServerResponse{
						Type:     "INCOMING_MSG",
						Network:  "whatsapp",
						Sender:   conv.GetID(),
						ChatName: chatName,
						Message:  "[Histórico sincronizado]",
					})
				}
			}
		case *events.Message:
			if activeClient != nil {
				var body string
				if evt.Message.GetConversation() != "" {
					body = evt.Message.GetConversation()
				} else if evt.Message.GetExtendedTextMessage() != nil {
					body = evt.Message.GetExtendedTextMessage().GetText()
				}

				if body != "" {
					chatName := evt.Info.PushName
					if chatName == "" {
						chatName = evt.Info.Sender.User
					}
					sendToClient(activeClient, ServerResponse{
						Type:     "INCOMING_MSG",
						Network:  "whatsapp",
						Sender:   evt.Info.Sender.User,
						ChatName: chatName,
						Message:  body,
					})
				}
			}
		}
	})
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
