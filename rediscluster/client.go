package rediscluster

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClusterClient provides a go-redis compatible interface with cluster failover
type ClusterClient struct {
	adapter *ClusterAdapter
}

// NewClusterClient creates a new cluster client wrapper
func NewClusterClient(adapter *ClusterAdapter) *ClusterClient {
	return &ClusterClient{
		adapter: adapter,
	}
}

// Redis String operations

// Get returns the value of key
func (c *ClusterClient) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "get", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Get(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Set sets key to hold the string value with optional expiration
func (c *ClusterClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, "set", key, value)
	if expiration > 0 {
		cmd = redis.NewStatusCmd(ctx, "set", key, value, "EX", int64(expiration.Seconds()))
	}

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		var result *redis.StatusCmd
		if expiration > 0 {
			result = client.Set(ctx, key, value, expiration)
		} else {
			result = client.Set(ctx, key, value, 0)
		}
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Del deletes the specified keys
func (c *ClusterClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, append([]interface{}{"del"}, stringSliceToInterfaceSlice(keys)...)...)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Del(ctx, keys...)
		cmd.SetVal(result.Val())
		return result.Err()
	})
	cmd.SetErr(err)
	return cmd
}

// Exists checks if keys exist
func (c *ClusterClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, append([]interface{}{"exists"}, stringSliceToInterfaceSlice(keys)...)...)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Exists(ctx, keys...)
		cmd.SetVal(result.Val())
		return result.Err()
	})
	cmd.SetErr(err)
	return cmd
}

// Expire sets a timeout on a key
func (c *ClusterClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx, "expire", key, int64(expiration.Seconds()))
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Expire(ctx, key, expiration)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// TTL returns the remaining time to live of a key
func (c *ClusterClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	cmd := redis.NewDurationCmd(ctx, time.Second, "ttl", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.TTL(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Redis Hash operations

// HGet returns the value associated with field in the hash stored at key
func (c *ClusterClient) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "hget", key, field)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.HGet(ctx, key, field)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// HSet sets the specified fields to their respective values in the hash stored at key
func (c *ClusterClient) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	args := make([]interface{}, 0, 2+len(values))
	args = append(args, "hset", key)
	args = append(args, values...)
	cmd := redis.NewIntCmd(ctx, args...)

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.HSet(ctx, key, values...)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// HGetAll returns all fields and values of the hash stored at key
func (c *ClusterClient) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	cmd := redis.NewMapStringStringCmd(ctx, "hgetall", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.HGetAll(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// HDel deletes one or more hash fields
func (c *ClusterClient) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	args := make([]interface{}, 0, 2+len(fields))
	args = append(args, "hdel", key)
	args = append(args, stringSliceToInterfaceSlice(fields)...)
	cmd := redis.NewIntCmd(ctx, args...)

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.HDel(ctx, key, fields...)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Redis Set operations

// SAdd adds the specified members to the set stored at key
func (c *ClusterClient) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	args := make([]interface{}, 0, 2+len(members))
	args = append(args, "sadd", key)
	args = append(args, members...)
	cmd := redis.NewIntCmd(ctx, args...)

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.SAdd(ctx, key, members...)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// SMembers returns all the members of the set value stored at key
func (c *ClusterClient) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx, "smembers", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.SMembers(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// SRem removes the specified members from the set stored at key
func (c *ClusterClient) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	args := make([]interface{}, 0, 2+len(members))
	args = append(args, "srem", key)
	args = append(args, members...)
	cmd := redis.NewIntCmd(ctx, args...)

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.SRem(ctx, key, members...)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// SCard returns the set cardinality (number of elements) of the set stored at key
func (c *ClusterClient) SCard(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "scard", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.SCard(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Redis List operations

// LPush inserts all the specified values at the head of the list stored at key
func (c *ClusterClient) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	args := make([]interface{}, 0, 2+len(values))
	args = append(args, "lpush", key)
	args = append(args, values...)
	cmd := redis.NewIntCmd(ctx, args...)

	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.LPush(ctx, key, values...)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// RPop removes and returns the last element of the list stored at key
func (c *ClusterClient) RPop(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "rpop", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.RPop(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// LLen returns the length of the list stored at key
func (c *ClusterClient) LLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "llen", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.LLen(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Redis utility operations

// Ping pings the Redis server
func (c *ClusterClient) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, "ping")
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Ping(ctx)
		cmd.SetVal(result.Val())
		return result.Err()
	})
	cmd.SetErr(err)
	return cmd
}

// Keys finds all keys matching the given pattern
func (c *ClusterClient) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx, "keys", pattern)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Keys(ctx, pattern)
		cmd.SetVal(result.Val())
		return result.Err()
	})
	cmd.SetErr(err)
	return cmd
}

// FlushDB removes all keys from the current database
func (c *ClusterClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, "flushdb")
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.FlushDB(ctx)
		cmd.SetVal(result.Val())
		return result.Err()
	})
	cmd.SetErr(err)
	return cmd
}

// Pipeline operations

// Pipeline returns a new pipeline
func (c *ClusterClient) Pipeline() redis.Pipeliner {
	// For now, return pipeline from primary client
	// TODO: Implement cluster-aware pipelining
	if c.adapter.primaryClient != nil {
		return c.adapter.primaryClient.Pipeline()
	}

	// If no primary client, try to get a healthy node
	if node := c.adapter.GetHealthyNode(); node != nil {
		node.mutex.RLock()
		client := node.client
		node.mutex.RUnlock()
		if client != nil {
			return client.Pipeline()
		}
	}

	// Return nil if no healthy nodes available
	return nil
}

// TxPipeline returns a new transaction pipeline
func (c *ClusterClient) TxPipeline() redis.Pipeliner {
	// For now, return transaction pipeline from primary client
	// TODO: Implement cluster-aware transaction pipelining
	if c.adapter.primaryClient != nil {
		return c.adapter.primaryClient.TxPipeline()
	}

	// If no primary client, try to get a healthy node
	if node := c.adapter.GetHealthyNode(); node != nil {
		node.mutex.RLock()
		client := node.client
		node.mutex.RUnlock()
		if client != nil {
			return client.TxPipeline()
		}
	}

	// Return nil if no healthy nodes available
	return nil
}

// Incr increments the number stored at key by one
func (c *ClusterClient) Incr(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "incr", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Incr(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// Decr decrements the number stored at key by one
func (c *ClusterClient) Decr(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "decr", key)
	err := c.adapter.ExecuteWithRetry(ctx, func(ctx context.Context, client *redis.Client) error {
		result := client.Decr(ctx, key)
		cmd.SetVal(result.Val())
		return result.Err()
	}, key)
	cmd.SetErr(err)
	return cmd
}

// WithContext returns a shallow copy of c with its context changed to ctx
func (c *ClusterClient) WithContext(ctx context.Context) *ClusterClient {
	// Return the same client since we use context in method calls
	return c
}

// Close closes the cluster client
func (c *ClusterClient) Close() error {
	return c.adapter.Close()
}

// Helper functions

func stringSliceToInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}
