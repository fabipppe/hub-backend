package main

import (
	"context"
	"database/sql"
	"encoding/base64"
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
	Type       string         `json:"type"`
	Network    string         `json:"network"`
	Status     string         `json:"status,omitempty"`
	Connected  bool           `json:"connected"`
	Payload    string         `json:"payload,omitempty"`
	Sender     string         `json:"sender,omitempty"`
	Message    string         `json:"message,omitempty"`
	ChatName   string         `json:"chatName,omitempty"`
	Attachment string         `json:"attachment,omitempty"` // Novo campo de Anexo
	Chats      []ChatEntry    `json:"chats,omitempty"`
	Messages   []HistoryEntry `json:"messages,omitempty"`
}

type ChatEntry struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	LastMessage string `json:"lastMessage"`
}

type HistoryEntry struct {
	ID         string `json:"id"`
	ChatJID    string `json:"chatJid"`
	ChatName   string `json:"chatName"`
	Text       string `json:"text"`
	Timestamp  int64  `json:"timestamp"`
	FromMe     bool   `json:"fromMe"`
	Attachment string `json:"attachment,omitempty"` // Novo campo de Anexo
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
	historyDB    *sql.DB
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

	fmt.Println("Beeper Hub com Imagens Ativo na porta " + port)
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
		log.Println("Erro SQLite WhatsMeow:", err)
		return
	}
	waContainer = container

	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		log.Println("Erro ao abrir base de dados para histórico:", err)
		return
	}
	historyDB = db

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS whatsapp_messages (
		msg_id TEXT PRIMARY KEY,
		chat_jid TEXT,
		chat_name TEXT,
		message_text TEXT,
		timestamp INTEGER,
		from_me BOOLEAN
	);`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Println("Erro ao criar tabela de mensagens:", err)
	}

	// Adicionar coluna 'attachment' em background, se não existir (Migração)
	_, _ = db.Exec("ALTER TABLE whatsapp_messages ADD COLUMN attachment TEXT DEFAULT ''")
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

			go func(c *Client) {
				time.Sleep(1 * time.Second)
				sendFullHistory(c)
			}(client)

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

	// Heartbeat periódico (15s)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
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

		norm := strings.ToLower(strings.TrimSpace(appMsg.Network))

		switch appMsg.Type {
		case "PING":
			sendToClient(client, ServerResponse{Type: "PONG", Network: norm, Connected: true})

		case "START_BRIDGE", "CONNECT_BRIDGE":
			switch norm {
			case "whatsapp":
				go startRealWhatsAppBridge(client)

			case "telegram":
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "Insira o número de telemóvel para autenticação",
					Connected: false,
				})

			case "messages", "google_messages", "sms":
				pngBytes, _ := qrcode.Encode("https://messages.google.com/web/authentication", qrcode.Medium, 256)
				payload := "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_QR",
					Network:   "messages",
					Status:    "Aponte a aplicação Google Mensagens para este código",
					Payload:   payload,
					Connected: false,
				})

			case "messenger":
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "messenger",
					Status:    "Aguardando sessão do Facebook Messenger",
					Connected: false,
				})

			default:
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   norm,
					Status:    "Pronto para ligar a " + norm,
					Connected: false,
				})
			}

		case "PAIR_PHONE":
			if norm == "whatsapp" {
				go handlePairPhoneWhatsApp(client, appMsg.Payload)
			} else if norm == "telegram" {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "Código de verificação enviado por SMS para " + appMsg.Payload,
					Connected: false,
				})
			}

		case "SEND_CODE":
			if norm == "telegram" {
				sendToClient(client, ServerResponse{
					Type:      "BRIDGE_STATUS",
					Network:   "telegram",
					Status:    "Conectado",
					Connected: true,
				})
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
					if historyDB != nil {
						_, _ = historyDB.Exec(
							"INSERT OR REPLACE INTO whatsapp_messages (msg_id, chat_jid, chat_name, message_text, timestamp, from_me) VALUES (?, ?, ?, ?, ?, ?)",
							fmt.Sprintf("sent_%d", time.Now().UnixNano()),
							jid.String(),
							recipient,
							appMsg.Payload,
							time.Now().UnixMilli(),
							true,
						)
					}
					fmt.Println("Mensagem enviada e guardada no histórico para:", recipient)
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

func sendFullHistory(client *Client) {
	waLock.Lock()
	defer waLock.Unlock()

	if historyDB == nil {
		return
	}

	rows, err := historyDB.Query("SELECT msg_id, chat_jid, chat_name, message_text, timestamp, from_me, IFNULL(attachment, '') FROM whatsapp_messages ORDER BY timestamp ASC")
	if err != nil {
		fmt.Println("Erro a ler mensagens do SQLite:", err)
		return
	}
	defer rows.Close()

	var messageList []HistoryEntry
	for rows.Next() {
		var m HistoryEntry
		if err := rows.Scan(&m.ID, &m.ChatJID, &m.ChatName, &m.Text, &m.Timestamp, &m.FromMe, &m.Attachment); err == nil {
			messageList = append(messageList, m)
		}
	}

	if len(messageList) > 0 {
		fmt.Printf("A enviar %d mensagens reais de histórico para a app!\n", len(messageList))
		sendToClient(client, ServerResponse{
			Type:      "HISTORY_MESSAGES",
			Network:   "whatsapp",
			Status:    "Histórico Completo",
			Connected: true,
			Messages:  messageList,
		})
	}
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
			Status:    "Conectado",
			Connected: true,
		})
		go sendFullHistory(client)
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
				pngBytes, err := qrcode.Encode(evt.Code, qrcode.Medium, 256)
				var payload string
				if err == nil {
					payload = "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
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
				go sendFullHistory(client)
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
					Status:    "Conectado",
					Connected: true,
				})
				go sendFullHistory(activeClient)
			}

		case *events.HistorySync:
			if evt.Data == nil || historyDB == nil {
				return
			}
			fmt.Printf("HistorySync recebido com %d conversas para gravar no histórico real!\n", len(evt.Data.GetConversations()))
			for _, conv := range evt.Data.GetConversations() {
				chatJID := conv.GetID()
				chatName := conv.GetName()
				if chatName == "" {
					chatName = chatJID
				}

				for _, histMsg := range conv.GetMessages() {
					webMsg := histMsg.GetMessage()
					if webMsg == nil {
						continue
					}

					rawMsg := webMsg.GetMessage()
					if rawMsg == nil {
						continue
					}

					var text string
					var base64Img string

					if rawMsg.GetConversation() != "" {
						text = rawMsg.GetConversation()
					} else if rawMsg.GetExtendedTextMessage() != nil {
						text = rawMsg.GetExtendedTextMessage().GetText()
					} else if imgMsg := rawMsg.GetImageMessage(); imgMsg != nil {
						text = imgMsg.GetCaption()
						if text == "" {
							text = "[Imagem]"
						}
						// Descarregar a imagem para renderizar na UI
						if data, err := waClient.Download(context.Background(), imgMsg); err == nil {
							mime := imgMsg.GetMimetype()
							if mime == "" {
								mime = "image/jpeg"
							}
							base64Img = "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(data)
						}
					}

					if text == "" && base64Img == "" {
						continue
					}

					msgID := webMsg.GetKey().GetID()
					fromMe := webMsg.GetKey().GetFromMe()
					ts := int64(webMsg.GetMessageTimestamp()) * 1000

					_, _ = historyDB.Exec(
						"INSERT OR IGNORE INTO whatsapp_messages (msg_id, chat_jid, chat_name, message_text, timestamp, from_me, attachment) VALUES (?, ?, ?, ?, ?, ?, ?)",
						msgID, chatJID, chatName, text, ts, fromMe, base64Img,
					)
				}
			}

			if activeClient != nil {
				go sendFullHistory(activeClient)
			}

		case *events.Message:
			var body string
			var base64Img string

			if evt.Message.GetConversation() != "" {
				body = evt.Message.GetConversation()
			} else if evt.Message.GetExtendedTextMessage() != nil {
				body = evt.Message.GetExtendedTextMessage().GetText()
			} else if imgMsg := evt.Message.GetImageMessage(); imgMsg != nil {
				body = imgMsg.GetCaption()
				if body == "" {
					body = "[Imagem]"
				}
				// Download da imagem em tempo real
				if data, err := waClient.Download(context.Background(), imgMsg); err == nil {
					mime := imgMsg.GetMimetype()
					if mime == "" {
						mime = "image/jpeg"
					}
					base64Img = "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(data)
				}
			}

			if body != "" || base64Img != "" {
				chatName := evt.Info.PushName
				if chatName == "" {
					chatName = evt.Info.Sender.User
				}
				chatJID := evt.Info.Chat.String()
				msgID := evt.Info.ID
				ts := evt.Info.Timestamp.UnixMilli()

				if historyDB != nil {
					_, _ = historyDB.Exec(
						"INSERT OR IGNORE INTO whatsapp_messages (msg_id, chat_jid, chat_name, message_text, timestamp, from_me, attachment) VALUES (?, ?, ?, ?, ?, ?, ?)",
						msgID, chatJID, chatName, body, ts, false, base64Img,
					)
				}

				if activeClient != nil {
					sendToClient(activeClient, ServerResponse{
						Type:       "INCOMING_MSG",
						Network:    "whatsapp",
						Sender:     chatJID,
						ChatName:   chatName,
						Message:    body,
						Attachment: base64Img,
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
