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

#ifndef OSIRIS_FILTER_H
#define OSIRIS_FILTER_H

#include "bfd.h"
#include "keys_utils.h"
#include <chrono>
#include <random>

#ifndef OSIRIS_HASH_CACHE_SIZE
#define OSIRIS_HASH_CACHE_SIZE 1024
#endif

namespace osiris
{
    static std::mt19937 osiris_rng((uint32_t)std::chrono::steady_clock::now().time_since_epoch().count());

    class OsirisFilter
    {
    protected:
        Dictionary* length[2] = {nullptr};

        Dictionary* links[2][32] = {nullptr};
        uint32_t links_mask[2] = {0, 0};

        uint32_t max_link_size_in_bits = 0;

        hash_t hash_seed = 0;

        hash_t hash_cache[OSIRIS_HASH_CACHE_SIZE] = { 0 };

        size_t extractLink(bool bit, hash_t hash, uint8_t* buffer)
        {
            size_t size = 0;
            length[bit]->get(hash, (uint8_t*)&size);

            for (int b = 31; b >= 3; b--)
            {
                if (size & (1ull << b))
                {
                    links[bit][b]->get(hash, buffer);
                    buffer += (1ull << (b - 3));
                }
            }

            if (size & 7)
            {
                uint8_t result = 0;
                uint8_t tmp;
                if (size & 1)
                {
                    tmp = 0;
                    links[bit][0]->get(hash, &tmp);
                    result |= tmp;
                }
                if (size & 2)
                {
                    tmp = 0;
                    links[bit][1]->get(hash, &tmp);
                    result = (result << 2) | tmp;
                }
                if (size & 4)
                {
                    tmp = 0;
                    links[bit][2]->get(hash, &tmp);
                    result = (result << 4) | tmp;
                }

                buffer[0] = result;
            }

            return size;
        }

        inline hash_t nextHash(hash_t seed, size_t id)
        {
            if (id < OSIRIS_HASH_CACHE_SIZE)
            {
                return hash_cache[id];
            }
            return nextRand(seed);
        }

        // implementation is too different

        virtual bool pointQueryInternal(const std::string& key, uint8_t* link_buffer) = 0;

        virtual bool prefixQueryInternal(const std::string& prefix, uint8_t* link_buffer) = 0;

        virtual bool rangeQueryInternal(const std::string& left, bool includeLeft,
                                        const std::string& right, bool includeRight,
                                        uint8_t* prefixBuffer, uint8_t* tailBuffer) = 0;

        // serialization

        virtual uint8_t getFilterId() = 0;

        virtual uint8_t* serializeExtra(uint8_t* buf) = 0;

        virtual size_t serializeExtraSize() = 0;

        uint8_t* serializeCore(uint8_t* buf)
        {
            uint8_t id = getFilterId();
            memmove(buf, &id, sizeof(id));
            buf += sizeof(id);
            memmove(buf, &hash_seed, sizeof(hash_seed));
            buf += sizeof(hash_seed);
            memmove(buf, &max_link_size_in_bits, sizeof(max_link_size_in_bits));
            buf += sizeof(max_link_size_in_bits);

            for (int i = 0; i < 2; ++i)
            {
                buf = length[i]->serialize(buf);

                memmove(buf, &links_mask[i], sizeof(links_mask[i]));
                buf += sizeof(links_mask[i]);

                for (int b = 0; b < 32; ++b)
                {
                    if (links_mask[i] & (1ull << b))
                    {
                        buf = links[i][b]->serialize(buf);
                    }
                }
            }
            return buf;
        }

        size_t getSerializationSize()
        {
            size_t total_size = 1;

            total_size += sizeof(hash_seed);
            total_size += sizeof(max_link_size_in_bits);

            for (int i = 0; i < 2; ++i)
            {
                total_size += length[i]->getSerializationSize();
                total_size += sizeof(links_mask[i]);
                for (int b = 0; b < 32; ++b)
                {
                    if (links_mask[i] & (1ull << b))
                    {
                        total_size += links[i][b]->getSerializationSize();
                    }
                }
            }
            return total_size + serializeExtraSize();
        }

        uint8_t* deserializeCore(uint8_t* buf)
        {
            memmove(&hash_seed, buf, sizeof(hash_seed));
            buf += sizeof(hash_seed);
            if (OSIRIS_HASH_CACHE_SIZE > 0)
            {
                hash_cache[0] = nextRand(hash_seed);
                for (int i = 1; i < OSIRIS_HASH_CACHE_SIZE; ++i)
                {
                    hash_cache[i] = nextRand(hash_cache[i - 1]);
                }
            }
            memmove(&max_link_size_in_bits, buf, sizeof(max_link_size_in_bits));
            buf += sizeof(max_link_size_in_bits);

            for (int i = 0; i < 2; ++i)
            {
                auto res = deserializeDictionary(buf);
                buf = res.second;
                length[i] = res.first;

                memmove(&links_mask[i], buf, sizeof(links_mask[i]));
                buf += sizeof(links_mask[i]);

                for (int b = 0; b < 32; ++b)
                {
                    if (links_mask[i] & (1ull << b))
                    {
                        res = deserializeDictionary(buf);
                        links[i][b] = res.first;
                        buf = res.second;
                    }
                    else
                    {
                        links[i][b] = nullptr;
                    }
                }
            }
            return buf;
        }

    public:

        bool pointQuery(const std::string& key)
        {
            uint8_t* link_buffer = nullptr;
            try
            {
                link_buffer = new uint8_t[(max_link_size_in_bits + 7) >> 3];
                bool result = pointQueryInternal(key, link_buffer);
                delete[] link_buffer;
                return result;
            }
            catch (std::bad_alloc&)
            {
                delete[] link_buffer;
                // could not allocate memory for query, so cannot ensure there is no key
                return true;
            }
        }

        bool prefixQuery(const std::string& prefix)
        {
            uint8_t* link_buffer = nullptr;
            try
            {
                link_buffer = new uint8_t[(max_link_size_in_bits + 7) >> 3];
                bool result = prefixQueryInternal(prefix, link_buffer);
                delete[] link_buffer;
                return result;
            }
            catch (std::bad_alloc&)
            {
                delete[] link_buffer;
                // could not allocate memory for query, so cannot ensure there is no key
                return true;
            }
        }

        bool rangeQuery(const std::string& left, bool include_left, const std::string& right, bool include_right)
        {
            int res = compareEndpoints(left, right);
            uint8_t* buf1 = nullptr;
            uint8_t* buf2 = nullptr;
            switch (res)
            {
                case -1:
                    try
                    {
                        buf1 = new uint8_t[(max_link_size_in_bits + 7) >> 3];
                        buf2 = new uint8_t[(max_link_size_in_bits + 7) >> 3];
                        bool result = rangeQueryInternal(left, include_left, right, include_right, buf1, buf2);
                        delete[] buf1;
                        delete[] buf2;
                        return result;
                    }
                    catch (std::bad_alloc&)
                    {
                        delete[] buf1;
                        delete[] buf2;
                        // could not allocate memory for query, so cannot ensure there is no key
                        return true;
                    }
                case 0:
                    return include_left && include_right && pointQuery(left);
                default:
                    return false;
            }
        }

        std::pair<uint8_t*, size_t> serialize()
        {
            size_t size = getSerializationSize();

            uint8_t* data = new uint8_t[size];

            uint8_t* cur = serializeCore(data);

            serializeExtra(cur);

            return { data, size };
        }

        virtual ~OsirisFilter()
        {
            for (size_t i = 0; i < 2; ++i)
            {
                delete length[i];
                for (int b = 0; b < 32; ++b)
                {
                    if (links_mask[i] & (1ull << b))
                    {
                        delete links[i][b];
                    }
                }
            }
        }
    };
}

#endif