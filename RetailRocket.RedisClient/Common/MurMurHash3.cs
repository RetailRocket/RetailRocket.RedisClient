using System.Text;

namespace RetailRocket.RedisClient.Common
{
    public static class MurMurHash3
    {
        public static int Hash(string data)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            return MurMurHash3x8632(bytes, 0, (uint)bytes.Length, 0);
        }

        public static int MurMurHash3x8632(byte[] data, uint offset, uint len, uint seed)
        {
            uint c1 = 0xcc9e2d51;
            uint c2 = 0x1b873593;

            uint k1 = 0;
            uint h1 = seed;
            uint roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

            for (uint i = offset; i < roundedEnd; i += 4)
            {
                // little endian load order
                k1 = (uint)((data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24));
                k1 *= c1;
                k1 = Rotl32(k1, 15);
                k1 *= c2;

                h1 ^= k1;
                h1 = Rotl32(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }

            // tail
            k1 = 0;

            switch (len & 0x03)
            {
                case 3:
                    k1 = (uint)((data[roundedEnd + 2] & 0xff) << 16);
                    goto case 2; // fallthrough
                case 2:
                    k1 |= (uint)((data[roundedEnd + 1] & 0xff) << 8);
                    goto case 1; // fallthrough
                case 1:
                    k1 |= (uint)(data[roundedEnd] & 0xff);
                    k1 *= c1;
                    k1 = Rotl32(k1, 15);
                    k1 *= c2;
                    h1 ^= k1;
                    break;
            }

            // finalization
            h1 ^= len;

            // fmix(h1);
            h1 ^= h1 >> 16;
            h1 *= 0x85ebca6b;
            h1 ^= h1 >> 13;
            h1 *= 0xc2b2ae35;
            h1 ^= h1 >> 16;

            return (int)(h1 & 0x7fffffff);
        }

        private static uint Rotl32(uint x, int r)
        {
            return (x << r) | (x >> (32 - r));
        }
    }
}