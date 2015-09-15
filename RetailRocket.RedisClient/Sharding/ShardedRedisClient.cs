using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Redis;
using ServiceStack.Redis.Generic;
using ServiceStack.Text;

namespace RetailRocket.RedisClient.Sharding
{
    public class ShardedRedisClient : IShardedRedisClient
    {
        private readonly ShardingStrategy shardingStrategy;
        private readonly IDictionary<IRedisClientsManager, IRedisClient> clientsCache = new Dictionary<IRedisClientsManager, IRedisClient>();

        public ShardedRedisClient(ShardingStrategy shardingStrategy)
        {
            this.shardingStrategy = shardingStrategy;
        }

        public List<T> Write<T>(string key, Func<IRedisClient, T> action)
        {
            var result = new List<T>();
            var managers = shardingStrategy.GetWriteManagers(key);
            foreach (var manager in managers)
                result.Add(action(GetCachedClient(manager)));

            return result;
        }

        public T Read<T>(string key, Func<IRedisClient, T> func)
        {
            var manager = shardingStrategy.GetReadManager(key);
            return func(GetCachedClient(manager));
        }

        public IList<T> ReadManyKeys<T>(IEnumerable<string> keys, Func<IRedisClient, List<string>, List<T>> func)
        {
            var groupedKeys = keys.ToLookup(key => shardingStrategy.GetReadManager(key));
            var result = new List<T>();
            foreach (var group in groupedKeys)
                result.AddRange(func(GetCachedClient(group.Key), group.ToList()));
            return result;
        }

        public IDictionary<string, T> ReadMap<T>(List<string> keys, Func<IRedisClient, List<string>, IDictionary<string, T>> func)
        {
            var groupedKeys = keys.ToLookup(key => shardingStrategy.GetReadManager(key));
            var result = new List<KeyValuePair<string, T>>();
            foreach (var group in groupedKeys)
                result.AddRange(func(GetCachedClient(group.Key), group.ToList()));
            return result.ToDictionary(a => a.Key, a => a.Value);
        }

        public bool Add<T>(string key, T value, TimeSpan expiresIn)
        {
            return Write(key, client => client.Add(key, value, expiresIn))
                .All(result => result);
        }

        public T Get<T>(string key)
        {
            return Read(key, client => client.Get<T>(key));
        }

        public string Get(string key)
        {
            return Read(key, client => client.GetValue(key));
        }

        public TimeSpan GetTimeToLive(string key)
        {
            return Read(key, client => client.GetTimeToLive(key));
        }

        public IRedisList<T> GetList<T>(string key)
        {
            return Read(key, client => client.As<T>().Lists[key]);
        }

        public bool Set<T>(string key, T value, TimeSpan expiresIn)
        {
            return Write(key, client => client.Set(key, value, expiresIn))
                .All(b => b);
        }

        public bool Set<T>(string key, T value)
        {
            return Write(key, client => client.Set(key, value))
                .All(b => b);
        }

        public void PushItemToList<T>(string key, T value)
        {
            Write(key, client =>
            {
                client.PushItemToList(key, value.SerializeToString());
                return true;
            });
        }

        public void SetEntry(string key, string value, TimeSpan expireIn)
        {
            Write(key, client =>
            {
                client.SetEntry(key, value, expireIn);
                return true;
            });
        }

        public bool Remove(string key)
        {
            return Write(key, client => client.Remove(key))
                .All(b => b);
        }

        public IList<string> GetValues(IEnumerable<string> keys)
        {
            return ReadManyKeys(keys, (client, localKeys) => client.GetValues(localKeys));
        }

        public void Rename(string key, string newKeyName)
        {
            Write(key, client => { client.RenameKey(key, newKeyName); return true; });
        }

        public IList<T> GetValues<T>(List<string> keys)
        {
            return ReadManyKeys(keys, (client, localKeys) => client.GetValues<T>(localKeys));
        }

        public IShardedRedisPipeline<T> CreatePipeline<T>()
        {
            return new ShardedRedisPipeline<T>(this, shardingStrategy);
        }

        public List<T> GetAllItemsFromList<T>(IRedisList<T> list)
        {
            return Read(list.Id, client => client.As<T>().GetAllItemsFromList(list));
        }

        public void AddItemToList<T>(IRedisList<T> list, T item)
        {
            Write(list.Id, client =>
            {
                client.As<T>().AddItemToList(list, item);
                return true;
            });
        }

        public void RemoveAllFromList(string key)
        {
            Write(key, client => { client.RemoveAllFromList(key); return true; });
        }

        public List<string> GetAllItemsFromList(string key)
        {
            return Read(key, client => client.GetAllItemsFromList(key));
        }

        public IDictionary<string, T> GetValuesMap<T>(List<string> keys)
        {
            return ReadMap(keys, (client, localKeys) => client.GetValuesMap<T>(localKeys));
        }

        public IRedisClient GetCachedClient(IRedisClientsManager manager)
        {
            if (!clientsCache.ContainsKey(manager))
                AddClientToCache(manager);
            return clientsCache[manager];
        }

        private void AddClientToCache(IRedisClientsManager manager)
        {
            clientsCache[manager] = manager.GetClient();
        }

        public Dictionary<TKey, TValue> GetAllEntriesFromHash<TKey, TValue>(string key)
        {
            return Read(key, client => client.GetAllEntriesFromHash(key).ToDictionary(k => (TKey)Convert.ChangeType(k.Key, typeof(TKey)), v => v.Value.FromJson<TValue>()));
        }

        public void SetEntryToHash<TValue>(string hashId, string key, TValue value)
        {
            Write(key, client => client.SetEntryInHash(hashId, key, value.ToJson()));
        }

        public void Dispose()
        {
            foreach (var client in clientsCache)
                client.Value.Dispose();
        }
    }
}
