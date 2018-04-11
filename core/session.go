package core

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"google.golang.org/grpc/metadata"
)

type Session struct {
	Token    string
	Username string
	CreateTS int64
	UpdateTS int64
}

type RedisPool struct {
	sync.Pool

	// 被用出去连接数
	ticket chan bool
}

func (p *RedisPool) GetConn() redis.Conn {
	p.ticket <- true
	return p.Get().(redis.Conn)
}

func (p *RedisPool) PutConn(conn redis.Conn) {
	<-p.ticket
	p.Put(conn)
}

type SessionManager struct {
	redisPool          *RedisPool
	onlineUsernamesKey string
}

func NewSessionManager(redisURL string) *SessionManager {
	return &SessionManager{
		redisPool: &RedisPool{
			sync.Pool{
				New: func() interface{} {
					conn, err := redis.DialURL(redisURL)
					if err != nil {
						log.Fatalln("redis.DialURL has error.", err)
					}
					return conn

				},
			}, make(chan bool, 10), // 最大redis连接数
		},
		onlineUsernamesKey: "online_usernames",
	}
}

// UserKey 根据用户名拼成一个redis key
func (sm *SessionManager) UserKey(username string) string {
	return fmt.Sprintf("user:%s", username)
}

// OnlineList 所有用户的在线列表(不一定精准)
func (sm *SessionManager) OnlineList() []string {
	conn := sm.redisPool.GetConn()
	defer sm.redisPool.PutConn(conn)
	usernames, err := redis.Strings(conn.Do("SMEMBERS", sm.onlineUsernamesKey))
	if err != nil {
		log.Println("An error occurred while calling SMEMBERS:", err)
		return []string{}
	}
	return usernames
}

// Get 根据用户名获取出session
func (sm *SessionManager) Get(username string) *Session {
	conn := sm.redisPool.GetConn()
	defer sm.redisPool.PutConn(conn)
	value, err := redis.Bytes(conn.Do("GET", sm.UserKey(username)))
	if err != nil {
		log.Printf("An error occurred while calling GET for %s: %v\n", sm.UserKey(username), err)
	}

	if value == nil {
		return nil
	}
	session := &Session{}
	if err = json.Unmarshal(value, session); err != nil {
		log.Println("An error occurred while calling json.Unmarshal:", err)
		return nil
	}
	return session
}

// GetFromContext 从context中获取session
func (sm *SessionManager) GetFromContext(ctx context.Context) *Session {
	md, _ := metadata.FromIncomingContext(ctx)
	usernames := md["username"]
	if len(usernames) == 0 {
		return nil
	}

	session := sm.Get(usernames[0])
	if session == nil {
		return nil
	}

	tokens := md["token"]
	if len(tokens) == 0 {
		return nil
	}
	if session.Token != tokens[0] {
		return nil
	}
	return session
}

// Offline 将某个用户下线. 删除session和在线列表中元素
func (sm *SessionManager) Offline(username string) {
	log.Println("offline", username)
	conn := sm.redisPool.GetConn()
	defer sm.redisPool.PutConn(conn)
	conn.Do("SREM", sm.onlineUsernamesKey, username)
}

func (sm *SessionManager) Create(username string) *Session {
	now := time.Now()
	session := &Session{
		CreateTS: now.UnixNano(),
		UpdateTS: now.UnixNano(),
		Username: username,
	}
	h := md5.New()
	h.Write([]byte(fmt.Sprint(now.UnixNano())))
	h.Write([]byte(fmt.Sprint(os.Getpid())))
	h.Write([]byte(fmt.Sprint(rand.Int63())))

	session.Token = fmt.Sprintf("%x", h.Sum(nil))
	conn := sm.redisPool.GetConn()
	defer sm.redisPool.PutConn(conn)
	if _, err := conn.Do("SADD", sm.onlineUsernamesKey, username); err != nil {
		log.Println("An error occurred while calling SADD:", err)
		return nil
	}

	value, _ := json.Marshal(session)
	if _, err := conn.Do("SETEX", sm.UserKey(username), 10, string(value)); err != nil {
		log.Printf("An error occurred calling SETEX %q: %v\n", sm.UserKey(username),
			err)
	}

	return session
}

// Exists 判断某个用户是否在线.
func (sm *SessionManager) Exists(username string) bool {
	conn := sm.redisPool.GetConn()
	if exists, err := redis.Bool(conn.Do("SISMEMBER", sm.onlineUsernamesKey, username)); err != nil {
		log.Println("An error occurred while calling SISMEMBER:", err)

	} else if !exists {
		sm.redisPool.PutConn(conn)
		return false
	}
	sm.redisPool.PutConn(conn)

	return sm.Get(username) != nil
}

// UpdateTTL session保活
func (sm *SessionManager) UpdateTTL(username string) {
	conn := sm.redisPool.GetConn()
	defer sm.redisPool.PutConn(conn)
	_, err := conn.Do("EXPIRE", sm.UserKey(username), 15)
	if err != nil {
		fmt.Println("update ttl error:", err)
	}
}
