/*
 * This file is part of OsirisFilter <https://github.com/aplyusnin/OsirisFilter>.
 * Copyright (C) 2024 Artem Plyusnin.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef OSIRIS_DICTIONARY_H
#define OSIRIS_DICTIONARY_H

#include "bfd_utils.h"

namespace osiris
{
    class Dictionary
    {
    //protected:
    public:
        uint8_t* data;
        DataLayout layout;

        // bytes for internal purposes
        uint8_t* tmp = nullptr;
        uint8_t* buf = nullptr;
    public:
        Dictionary(size_t keys, size_t bits_per_value)
        {
            layout = prepareLayout(keys, bits_per_value);
            data = new uint8_t[layout.total_size_in_bytes];
            tmp = new uint8_t[BITS_TO_BYTES(bits_per_value)];
            buf = new uint8_t[BITS_TO_BYTES(bits_per_value)];
        }

        virtual ~Dictionary()
        {
            delete[] data;
            delete[] tmp;
            delete[] buf;
        }

        virtual void populate(std::pair<location_t, uint8_t*>* vals, size_t* pos, size_t* id) = 0;

        virtual void getData(size_t pos, uint8_t* to) = 0;
        virtual void setData(size_t pos, uint8_t* from) = 0;
    public:

        bool build(hash_t* hashes, std::vector<std::pair<size_t, size_t>>& keys)
        {
            size_t size = keys.size();
            auto* pos = new size_t[size];
            memset(pos, 0, sizeof(size_t) * size);
            auto* id = new size_t[size];
            memset(id, 0, sizeof(size_t) * size);
            auto* sets = new size_t[layout.total_pages * 2];
            memset(sets, 0, sizeof(size_t) * layout.total_pages * 2);
            auto* st = new size_t[layout.total_pages * 2];
            memset(st, 0, sizeof(size_t) * layout.total_pages);

            auto* vals = new std::pair<location_t, uint8_t*>[size];

            memset(data, 0, layout.total_size_in_bytes);

            for (size_t i = 0; i < size; i++)
            {
                vals[i].first = hashToLocation(hashes[keys[i].first], layout);
                vals[i].second = (uint8_t*)&keys[i].second;
            }

            if (!peel(vals, size, pos, id, sets, st, layout))
            {
                delete[] pos;
                delete[] id;
                delete[] sets;
                delete[] st;
                delete[] vals;
                return false;
            }
            // use length-specific implementation of population function
            populate(vals, pos, id);

            delete[] pos;
            delete[] id;
            delete[] sets;
            delete[] st;
            delete[] vals;
            return true;
        }

        bool build(hash_t* hashes, std::vector<std::pair<size_t, bitstring>>& keys)
        {
            size_t size = keys.size();
            auto* pos = new size_t[size];
            memset(pos, 0, sizeof(size_t) * size);
            auto* id = new size_t[size];
            memset(id, 0, sizeof(size_t) * size);
            auto* sets = new size_t[layout.total_pages * 2];
            memset(sets, 0, sizeof(size_t) * layout.total_pages * 2);
            auto* st = new size_t[layout.total_pages * 2];
            memset(st, 0, sizeof(size_t) * layout.total_pages);

            auto* vals = new std::pair<location_t, uint8_t*>[size];

            memset(data, 0, layout.total_size_in_bytes);

            for (size_t i = 0; i < size; i++)
            {
                vals[i].first = hashToLocation(hashes[keys[i].first], layout);
                vals[i].second = keys[i].second.data();
            }

            if (!peel(vals, size, pos, id, sets, st, layout))
            {
                delete[] pos;
                delete[] id;
                delete[] sets;
                delete[] st;
                delete[] vals;
                return false;
            }
            // use length-specific implementation of population function
            populate(vals, pos, id);

            delete[] pos;
            delete[] id;
            delete[] sets;
            delete[] st;
            delete[] vals;
            return true;
        }

        virtual void get(hash_t hash, uint8_t* result) = 0;

        size_t getSerializationSize()
        {
            return sizeof(layout.keys) + sizeof(layout.len_in_bits) + layout.total_size_in_bytes;
        }

        uint8_t* serialize(uint8_t* buf)
        {
            memmove(buf, &layout.keys, sizeof(layout.keys));
            buf += sizeof(layout.keys);
            memmove(buf, &layout.len_in_bits, sizeof(layout.len_in_bits));
            buf += sizeof(layout.len_in_bits);
            memmove(buf, data, layout.total_size_in_bytes);
            buf += layout.total_size_in_bytes;
            return buf;
        }

    };

    class BitDictionary : public Dictionary
    {
        void populate(std::pair<location_t, uint8_t*>* vals, size_t* pos, size_t* id) override
        {
            memset(this->data, 0, this->layout.total_size_in_bytes);

            size_t p = layout.keys;

            while (p)
            {
                p--;

                uint32_t v = id[p];
                uint64_t u = pos[p];

                buf[0] = vals[v].second[0];
                for (size_t i : vals[v].first.position)
                {
                    getData(i, tmp);
                    buf[0] ^= tmp[0];
                }
                buf[0] &= 1;
                setData(u, buf);
            }
        }

        void get(hash_t hash, uint8_t* result) override
        {
            auto loc = hashToLocation(hash, layout);
            getData(loc.position[0], result);
            for (int it = 1; it < 4; ++it)
            {
                getData(loc.position[it], tmp);
                result[0] ^= tmp[0];
            }
            result[0] &= 1;
        }

        void getData(size_t pos, uint8_t* to) override
        {
            to[0] = (data[pos >> 3] >> (pos & 7));
        }

        void setData(size_t pos, uint8_t* from) override
        {
            size_t val = 1ull << (pos & 7);
            data[pos >> 3] &= ~val;
            data[pos >> 3] |= val * from[0];
        }
    public:

        BitDictionary(uint32_t keys, uint32_t bits_per_key) : Dictionary(keys, bits_per_key) {}

        static std::pair<BitDictionary*, uint8_t*> deserialize(uint32_t keys, uint32_t bits_per_key, uint8_t* buf)
        {
            auto* res = new BitDictionary(keys, bits_per_key);

            memmove(res->data, buf, res->layout.total_size_in_bytes);
            buf += res->layout.total_size_in_bytes;
            return { res, buf };
        }
    };

    class TwoBitDictionary : public Dictionary
    {

        void populate(std::pair<location_t, uint8_t*>* vals, size_t* pos, size_t* id) override
        {
            memset(this->data, 0, this->layout.total_size_in_bytes);

            size_t p = layout.keys;

            while (p)
            {
                p--;

                uint32_t v = id[p];
                uint64_t u = pos[p];

                buf[0] = vals[v].second[0];
                for (size_t i : vals[v].first.position)
                {
                    getData(i, tmp);
                    buf[0] ^= tmp[0];
                }
                buf[0] &= 3;
                setData(u, buf);
            }
        }

        void get(hash_t hash, uint8_t* result) override
        {
            auto loc = hashToLocation(hash, layout);
            getData(loc.position[0], result);
            for (int it = 1; it < 4; ++it)
            {
                getData(loc.position[it], tmp);
                result[0] ^= tmp[0];
            }
            result[0] &= 3;
        }

        void getData(size_t pos, uint8_t* to) override
        {
            to[0] = (data[pos >> 2] >> ((pos & 3) << 1));
        }

        void setData(size_t pos, uint8_t* from) override
        {
            data[pos >> 2] &= ~(3 << ((pos & 3) << 1));
            data[pos >> 2] |= from[0] << ((pos & 3) << 1);
        }

    public:

        TwoBitDictionary(uint32_t keys, uint32_t bits_per_key) : Dictionary(keys, bits_per_key) {}

        static std::pair<TwoBitDictionary*, uint8_t*> deserialize(uint32_t keys, uint32_t bits_per_key, uint8_t* buf)
        {
            auto* res = new TwoBitDictionary(keys, bits_per_key);

            memmove(res->data, buf, res->layout.total_size_in_bytes);
            buf += res->layout.total_size_in_bytes;
            return { res, buf };
        }
    };

    class FourBitDictionary : public  Dictionary
    {

        void populate(std::pair<location_t, uint8_t*>* vals, size_t* pos, size_t* id) override
        {
            memset(this->data, 0, this->layout.total_size_in_bytes);

            size_t p = layout.keys;

            while (p)
            {
                p--;

                uint32_t v = id[p];
                uint64_t u = pos[p];

                buf[0] = vals[v].second[0];
                for (size_t i : vals[v].first.position)
                {
                    getData(i, tmp);
                    buf[0] ^= tmp[0];
                }
                buf[0] &= 15;
                setData(u, buf);
            }
        }

        void get(hash_t hash, uint8_t* result) override
        {
            auto loc = hashToLocation(hash, layout);
            getData(loc.position[0], result);
            for (int it = 1; it < 4; ++it)
            {
                getData(loc.position[it], tmp);
                result[0] ^= tmp[0];
            }
            result[0] &= 15;
        }

        void getData(size_t pos, uint8_t* to) override
        {
            to[0] = (data[pos >> 1] >> ((pos & 1) << 2));
        }

        void setData(size_t pos, uint8_t* from) override
        {
            data[pos >> 1] &= ~(15 << ((pos & 1) << 2));
            data[pos >> 1] |= from[0] << ((pos & 1) << 2);
        }

    public:

        FourBitDictionary(uint32_t keys, uint32_t bits_per_key) : Dictionary(keys, bits_per_key) {}

        static std::pair<FourBitDictionary*, uint8_t*> deserialize(uint32_t keys, uint32_t bits_per_key, uint8_t* buf)
        {
            auto* res = new FourBitDictionary(keys, bits_per_key);

            memmove(res->data, buf, res->layout.total_size_in_bytes);
            buf += res->layout.total_size_in_bytes;
            return { res, buf };
        }
    };

    class ByteDictionary : public Dictionary
    {

        void populate(std::pair<location_t, uint8_t*>* vals, size_t* pos, size_t* id) override
        {
            memset(this->data, 0, this->layout.total_size_in_bytes);

            size_t p = layout.keys;

            while (p)
            {
                p--;

                uint32_t v = id[p];
                uint64_t u = pos[p];

                memcpy(buf, vals[v].second, layout.len_in_bytes);

                for (size_t i : vals[v].first.position)
                {
                    getData(i, tmp);
                    for (size_t pt = 0; pt < layout.len_in_bytes; ++pt)
                    {
                        buf[pt] ^= tmp[pt];
                    }
                }
                setData(u, buf);
            }
        }

        void get(hash_t hash, uint8_t* result) override
        {
            auto loc = hashToLocation(hash, layout);
            if (layout.len_in_bytes == 1)
            {
                uint8_t a = data[loc.position[0]];
                uint8_t b = data[loc.position[1]];
                uint8_t c = data[loc.position[2]];
                uint8_t d = data[loc.position[3]];

                result[0] = a ^ b ^ c ^ d;
                return;
            }

            memmove(result, data + loc.position[0] * layout.len_in_bytes, layout.len_in_bytes);
            size_t off;
            size_t s1 = loc.position[1] * layout.len_in_bytes;
            for (off = 0; off + 3 < layout.len_in_bytes; off += 4)
            {
                result[off + 0] ^= data[s1 + off];
                result[off + 1] ^= data[s1 + off + 1];
                result[off + 2] ^= data[s1 + off + 2];
                result[off + 3] ^= data[s1 + off + 3];
            }
            while (off < layout.len_in_bytes)
            {
                result[off] ^= data[s1 + off];
                off++;
            }

            s1 = loc.position[2] * layout.len_in_bytes;

            for (off = 0; off + 3 < layout.len_in_bytes; off += 4)
            {
                result[off + 0] ^= data[s1 + off];
                result[off + 1] ^= data[s1 + off + 1];
                result[off + 2] ^= data[s1 + off + 2];
                result[off + 3] ^= data[s1 + off + 3];
            }
            while (off < layout.len_in_bytes)
            {
                result[off] ^= data[s1 + off];
                off++;
            }

            s1 = loc.position[3] * layout.len_in_bytes;

            for (off = 0; off + 3 < layout.len_in_bytes; off += 4)
            {
                result[off + 0] ^= data[s1 + off];
                result[off + 1] ^= data[s1 + off + 1];
                result[off + 2] ^= data[s1 + off + 2];
                result[off + 3] ^= data[s1 + off + 3];
            }
            while (off < layout.len_in_bytes)
            {
                result[off] ^= data[s1 + off];
                off++;
            }
        }

        void getData(size_t pos, uint8_t* to) override
        {
            memmove(to, data + pos * layout.len_in_bytes, layout.len_in_bytes);
        }

        void setData(size_t pos, uint8_t* from) override
        {
            memmove(data + pos * layout.len_in_bytes, from, layout.len_in_bytes);
        }

    public:

        ByteDictionary(uint32_t keys, uint32_t bits_per_key) : Dictionary(keys, bits_per_key) {}

        static std::pair<ByteDictionary*, uint8_t*> deserialize(uint32_t keys, uint32_t bits_per_key, uint8_t* buf)
        {
            auto* res = new ByteDictionary(keys, bits_per_key);

            memmove(res->data, buf, res->layout.total_size_in_bytes);
            buf += res->layout.total_size_in_bytes;
            return { res, buf };
        }
    };

    inline Dictionary* initDictionary(size_t keys, size_t bits_per_entry)
    {
        switch (bits_per_entry)
        {
            case 1:
                return new BitDictionary(keys, bits_per_entry);
            case 2:
                return new TwoBitDictionary(keys, bits_per_entry);
            case 4:
                return new FourBitDictionary(keys, bits_per_entry);
            default:
                return new ByteDictionary(keys, bits_per_entry);
        }
    }

    inline std::pair<Dictionary*, uint8_t*> deserializeDictionary(uint8_t* buf)
    {
        uint32_t keys;
        uint32_t bits_per_entry;
        memmove(&keys, buf, sizeof(keys));
        buf += sizeof(keys);
        memmove(&bits_per_entry, buf, sizeof(bits_per_entry));
        buf += sizeof(bits_per_entry);

        switch (bits_per_entry)
        {
            case 1:
                return BitDictionary::deserialize(keys, bits_per_entry, buf);
            case 2:
                return TwoBitDictionary::deserialize(keys, bits_per_entry, buf);
            case 4:
                return FourBitDictionary::deserialize(keys, bits_per_entry, buf);
            default:
                return ByteDictionary::deserialize(keys, bits_per_entry, buf);
        }
    }

}

#endif