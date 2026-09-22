package queue

import (
	"context"
	"strconv"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/redis/go-redis/v9"
)

// enqueueScript atomically adds a task to a ready list only if it is not
// already queued. KEYS[1]=list KEYS[2]=membership-set ARGV[1]=id [ARGV[2]]="HEAD"
var enqueueScript = redis.NewScript(`
local list, members, id = KEYS[1], KEYS[2], ARGV[1]
if redis.call('SADD', members, id) == 0 then
	return 0
end
if ARGV[2] == 'HEAD' then
	redis.call('LPUSH', list, id)
else
	redis.call('RPUSH', list, id)
end
return 1
`)

// dequeueScript pops an id and clears its membership marker.
var dequeueScript = redis.NewScript(`
local id = redis.call('LPOP', KEYS[1])
if not id then return false end
redis.call('SREM', KEYS[2], id)
return id
`)

// popDueScript removes and returns due members of a sorted set. Scores use
// unix milliseconds for sub-second precision (no early promotion at a second
// boundary).
var popDueScript = redis.NewScript(`
local zset = KEYS[1]
local now, limit = ARGV[1], tonumber(ARGV[2])
local ids = redis.call('ZRANGEBYSCORE', zset, '-inf', now, 'LIMIT', 0, limit)
if #ids > 0 then
	redis.call('ZREM', zset, unpack(ids))
end
return ids
`)

// RedisQueue implements Queue on top of Redis.
type RedisQueue struct {
	rdb redis.UniversalClient
}

func NewRedisQueue(rdb redis.UniversalClient) *RedisQueue {
	return &RedisQueue{rdb: rdb}
}

func keys(p domain.Priority) (list, members string) {
	return "ready:list:" + string(p), "ready:members:" + string(p)
}

func waitKey(k WaitKind) string { return "wait:" + string(k) }

func (q *RedisQueue) Enqueue(ctx context.Context, p domain.Priority, taskID string) error {
	list, members := keys(p)
	return q.runEnqueue(ctx, list, members, taskID, false)
}

func (q *RedisQueue) EnqueueHead(ctx context.Context, p domain.Priority, taskID string) error {
	list, members := keys(p)
	return q.runEnqueue(ctx, list, members, taskID, true)
}

func (q *RedisQueue) runEnqueue(ctx context.Context, list, members, id string, head bool) error {
	flag := ""
	if head {
		flag = "HEAD"
	}
	return enqueueScript.Run(ctx, q.rdb, []string{list, members}, id, flag).Err()
}

func (q *RedisQueue) Dequeue(ctx context.Context, p domain.Priority) (string, error) {
	list, members := keys(p)
	res, err := dequeueScript.Run(ctx, q.rdb, []string{list, members}).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	s, ok := res.(string)
	if !ok {
		return "", nil
	}
	return s, nil
}

func (q *RedisQueue) Depth(ctx context.Context, p domain.Priority) (int64, error) {
	list, _ := keys(p)
	n, err := q.rdb.LLen(ctx, list).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

func (q *RedisQueue) AllDepths(ctx context.Context) (map[domain.Priority]int64, error) {
	pipe := q.rdb.Pipeline()
	cmds := make(map[domain.Priority]*redis.IntCmd)
	for _, p := range domain.PriorityOrder {
		list, _ := keys(p)
		cmds[p] = pipe.LLen(ctx, list)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	out := make(map[domain.Priority]int64, len(domain.PriorityOrder))
	for p, cmd := range cmds {
		out[p] = cmd.Val()
	}
	return out, nil
}

func (q *RedisQueue) AddWaiting(ctx context.Context, kind WaitKind, taskID string, dueAt time.Time) error {
	return q.rdb.ZAdd(ctx, waitKey(kind), redis.Z{
		Score:  float64(dueAt.UnixMilli()),
		Member: taskID,
	}).Err()
}

func (q *RedisQueue) RemoveWaiting(ctx context.Context, kind WaitKind, taskID string) error {
	return q.rdb.ZRem(ctx, waitKey(kind), taskID).Err()
}

// RemoveWaitingKind accepts the wait-set kind as a plain string (API shim).
func (q *RedisQueue) RemoveWaitingKind(ctx context.Context, kind, taskID string) error {
	return q.rdb.ZRem(ctx, waitKey(WaitKind(kind)), taskID).Err()
}

func (q *RedisQueue) PopDue(ctx context.Context, kind WaitKind, now time.Time, limit int64) ([]string, error) {
	res, err := popDueScript.Run(ctx, q.rdb,
		[]string{waitKey(kind)}, strconv.FormatInt(now.UnixMilli(), 10), limit).Result()
	if err != nil {
		return nil, err
	}
	raw, ok := res.([]interface{})
	if !ok {
		return nil, nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func (q *RedisQueue) WaitingDepth(ctx context.Context, kind WaitKind) (int64, error) {
	n, err := q.rdb.ZCard(ctx, waitKey(kind)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

// Ping checks connectivity.
func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}
