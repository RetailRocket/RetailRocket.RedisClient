using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Redis;
using ServiceStack.Redis.Generic;

namespace RetailRocket.RedisClient.Sharding
{
    public class ShardedRedisPipeline<T> : IShardedRedisPipeline<T>
    {
        private readonly ShardedRedisClient shardedClient;
        private readonly ShardingStrategy shardingStrategy;
        private readonly IDictionary<IRedisClientsManager, IRedisTypedPipeline<T>> pipelineCache = new Dictionary<IRedisClientsManager, IRedisTypedPipeline<T>>();

        public ShardedRedisPipeline(ShardedRedisClient shardedClient, ShardingStrategy shardingStrategy)
        {
            this.shardedClient = shardedClient;
            this.shardingStrategy = shardingStrategy;
        }

        public void Dispose()
        {
            foreach (var item in pipelineCache)
                item.Value.Dispose();
        }

        public void Flush()
        {
            foreach (var item in pipelineCache)
                item.Value.Flush();
        }

        public void QueueWriteCommand(string key, Action<IRedisTypedClient<T>> command)
        {
            var pipelines = GetWritePipelines(key);
            foreach (var pipeline in pipelines)
                pipeline.QueueCommand(command);
        }

        public void QueueReadCommand(string key, Func<IRedisTypedClient<T>, List<T>> command, Action<List<T>> callback)
        {
            var pipeline = GetReadPipeline(key);
            pipeline.QueueCommand(command, callback);
        }

        public void QueueReadCommand(string key, Func<IRedisTypedClient<T>, bool> command, Action<bool> callback)
        {
            var pipeline = GetReadPipeline(key);
            pipeline.QueueCommand(command, callback);
        }

        private IRedisTypedPipeline<T> GetReadPipeline(string key)
        {
            var manager = shardingStrategy.GetReadManager(key);
            return GetPipeline(manager);
        }

        private IEnumerable<IRedisTypedPipeline<T>> GetWritePipelines(string key)
        {
            var managers = shardingStrategy.GetWriteManagers(key);
            return managers.Select(GetPipeline);
        }

        private IRedisTypedPipeline<T> GetPipeline(IRedisClientsManager manager)
        {
            if (!pipelineCache.ContainsKey(manager))
                CreatePipeline(manager);
            return pipelineCache[manager];
        }

        private void CreatePipeline(IRedisClientsManager manager)
        {
            var client = shardedClient.GetCachedClient(manager);
            pipelineCache[manager] = client.As<T>().CreatePipeline();
        }
    }
}