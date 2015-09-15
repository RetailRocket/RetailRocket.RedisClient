using System;
using System.Collections.Generic;
using ServiceStack.Redis.Generic;

namespace RetailRocket.RedisClient
{
    public interface IShardedRedisPipeline<T> : IDisposable
    {
        void Flush();
        void QueueWriteCommand(string key, Action<IRedisTypedClient<T>> action);
        void QueueReadCommand(string key, Func<IRedisTypedClient<T>, List<T>> func, Action<List<T>> callback);
        void QueueReadCommand(string key, Func<IRedisTypedClient<T>, bool> func, Action<bool> callback);
    }
}