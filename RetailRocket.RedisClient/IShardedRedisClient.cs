using System;
using System.Collections.Generic;
using ServiceStack.Redis.Generic;

namespace RetailRocket.RedisClient
{
    public interface IShardedRedisClient : IDisposable
    {
        bool Add<T>(string key, T value, TimeSpan expiresIn);
        T Get<T>(string key);
        string Get(string key);
        IRedisList<T> GetList<T>(string key);
        bool Set<T>(string key, T value, TimeSpan expiresIn);
        bool Set<T>(string key, T value);
        bool Remove(string key);
        IList<string> GetValues(IEnumerable<string> keys);
        IList<T> GetValues<T>(List<string> keys);
        TimeSpan GetTimeToLive(string key);
        IShardedRedisPipeline<T> CreatePipeline<T>();
        List<T> GetAllItemsFromList<T>(IRedisList<T> list);
        void AddItemToList<T>(IRedisList<T> list, T item);
        void RemoveAllFromList(string key);
        List<string> GetAllItemsFromList(string key);
        Dictionary<TKey, TVal> GetAllEntriesFromHash<TKey, TVal>(string key);
        IDictionary<string, T> GetValuesMap<T>(List<string> keys);
        void Rename(string key, string newKeyName);
        void SetEntryToHash<TValue>(string hashId, string key, TValue value);
        void PushItemToList<TValue>(string key, TValue value);
        void SetEntry(string key, string value, TimeSpan expireIn);
    }
}