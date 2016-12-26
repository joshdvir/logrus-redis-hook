package logredis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

// RedisHook to sends logs to Redis server
type RedisHook struct {
	RedisPool *redis.Pool
	RedisHost string
	RedisKey  string
	RedisPort int
}

// NewHook creates a hook to be added to an instance of logger
func NewHook(redisHost string, port int, key string) (*RedisHook, error) {
	pool := newRedisConnectionPool(redisHost, port)

	// test if connection with REDIS can be established
	conn := pool.Get()
	defer conn.Close()

	// check connection
	_, err := conn.Do("PING")
	if err != nil {
		return nil, fmt.Errorf("unable to connect to REDIS: %s", err)
	}

	return &RedisHook{
		RedisHost: redisHost,
		RedisPool: pool,
		RedisKey:  key,
		RedisPort: port,
	}, nil
}

// Fire is called when a log event is fired.
func (hook *RedisHook) Fire(entry *logrus.Entry) error {
	var msg interface{}

	msg = createMessage(entry)

	js, err := json.Marshal(msg)
	// fmt.Println(string(js))
	if err != nil {
		return fmt.Errorf("error creating message for REDIS: %s", err)
	}

	conn := hook.RedisPool.Get()
	defer conn.Close()

	_, err = conn.Do("RPUSH", hook.RedisKey, js)
	if err != nil {
		return fmt.Errorf("error sending message to REDIS: %s", err)
	}
	return nil
}

// Levels returns the available logging levels.
func (hook *RedisHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}

func createMessage(entry *logrus.Entry) map[string]interface{} {
	m := make(map[string]interface{})
	m["@timestamp"] = entry.Time.UTC().Format(time.RFC3339Nano)
	// m["message"] = entry.Message
	for k, v := range entry.Data {
		m[k] = v
	}
	return m
}

func newRedisConnectionPool(server string, port int) *redis.Pool {
	hostPort := fmt.Sprintf("%s:%d", server, port)
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   2000,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", hostPort)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
