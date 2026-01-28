package adapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Nemutagk/godb/v2/definitions/adapter"
	"github.com/Nemutagk/godb/v2/definitions/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongodbAdapter struct {
	Name     string
	DbConfig config.Config
	RawConn  *mongo.Client
	*adapter.Config
	mu sync.Mutex
}

func NewAdapter() (*MongodbAdapter, error) {
	return &MongodbAdapter{
		Name:   "",
		Config: &adapter.Config{},
	}, nil
}

func (m *MongodbAdapter) SetConf(name string, config config.Config) error {
	useSrv := false
	if _, ok := config.Params["srv"]; ok && useSrv {
		useSrv = true
	}

	uri := ""
	if !useSrv {
		dbAuth := "admin"

		if dbAuthName, ok := config.Params["auth"]; ok {
			dbAuth = dbAuthName.(string)
		}

		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d/?authSource=%s",
			config.User,
			config.Password,
			config.Host,
			config.Port,
			dbAuth,
		)
	} else {
		uri = fmt.Sprintf("mongodb+srv://%s:%s@%s",
			config.User,
			config.Password,
			config.Host,
		)
	}

	m.Dsn = uri
	m.DbConfig = config
	return nil
}

func (m *MongodbAdapter) Connect() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if m.Dsn == "" {
		return fmt.Errorf("missing dsn for connection")
	}

	opts := options.Client().ApplyURI(m.Dsn)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	dbConn := client.Database(m.DbConfig.Database)

	m.RawConn = client
	m.Conn = dbConn

	return nil
}

func (m *MongodbAdapter) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Conn == nil {
		return fmt.Errorf("no active connection")
	}

	client := m.Conn.(*mongo.Client)
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	return nil
}

func (m *MongodbAdapter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Conn == nil {
		return nil
	}

	client := m.Conn.(*mongo.Client)
	if err := client.Disconnect(context.Background()); err != nil {
		return err
	}

	m.Conn = nil
	return nil
}

func (m *MongodbAdapter) GetConnection() any {
	return m.Conn
}

func (m *MongodbAdapter) GetRawConnection() *mongo.Client {
	return m.RawConn
}
