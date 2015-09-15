using System;
using RetailRocket.RedisClient.Common;
using ServiceStack.Redis;

namespace RetailRocket.RedisClient.Sharding
{
    public class ShardingStrategy
    {
        private readonly IRedisClientsManager[][] managers;
        private readonly Random random = new Random();

        public ShardingStrategy(IRedisClientsManager[][] managers)
        {
            this.managers = managers;
        }

        public IRedisClientsManager GetReadManager(string key)
        {
            var writeManagers = GetWriteManagers(key);
            // read from random mirror host
            return writeManagers[0];
        }

        public IRedisClientsManager[] GetWriteManagers(string key)
        {
            var index = Hash(key) % managers.Length;
            return managers[index];
        }

        private int Hash(string key)
        {
            return MurMurHash3.Hash(key);
        }
    }
}