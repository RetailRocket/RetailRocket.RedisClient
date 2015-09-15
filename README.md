# RetailRocket.RedisClient

How To Use

```csharp

var hosts = new[] { "redis1.host.com", "redis2.host.com", "redis3.host.com" };

var client = new ShardedRedisClient(
    new ShardingStrategy(new[]
    {
        hosts
            .Select(h => (IRedisClientsManager) new PooledRedisClientManager(5000, 2, h))
            .ToArray()
    })
);

var val = client.Get("key");
```
