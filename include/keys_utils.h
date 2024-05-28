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

#ifndef OSIRIS_KEYS_UTILS_H
#define OSIRIS_KEYS_UTILS_H

#include "bitstring.h"
#include "utils.h"

#define EXTRACT_BIT(word, pos) (((word)[(pos) >> 3] & (1 << (7 ^ ((pos) & 7)))))

namespace osiris
{

    inline void consumeLink(std::vector<std::pair<size_t, bitstring>>* link_chunks, const std::string& key, size_t start, size_t link_len, size_t id)
    {
        size_t pt = start;
        for (int b = 31; b >= 0; --b)
        {
            size_t block = (1ull << b);
            if (link_len & block)
            {
                auto& bs = link_chunks[b].emplace_back(id, bitstring::select(block));
                size_t off = 0;
                while (off < block && (pt & 7))
                {
                    bs.second.set(off, EXTRACT_BIT(key, pt));
                    off++;
                    pt++;
                }
                while (off + 8 < block)
                {
                    bs.second.setByte(off, rev_bit[key[pt >> 3]]);
                    off += 8;
                    pt += 8;
                }
                while (off < block)
                {
                    bs.second.set(off, EXTRACT_BIT(key, pt));
                    off++;
                    pt++;
                }
            }
        }
    }

    struct FixedKeySetData
    {
        size_t id;
        size_t max_link_length = 0;
        std::vector<std::pair<size_t, size_t>> link_lengths[2];
        std::vector<std::pair<size_t, bitstring>> link_chunks[2][32];
        hash_t* hashes;
    };



    inline void collectDataAndHashes(FixedKeySetData& key_set_data, const std::vector<std::string>& keys,
                              size_t l, size_t r, size_t pos,
                              hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        if (l1 < l)
        {
            // do nothing
        }

        if (r < l) return;

        if (l == r)
        {
            size_t length = keys[l].size() << 3;

            size_t link_length = length - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);
            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);

            size_t link_length = next_pos - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);

        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[0].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[0], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[1].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[1], keys[m + 1], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }

    inline void collectHashes(FixedKeySetData& key_set_data, const std::vector<std::string>& keys,
                                     size_t l, size_t r, size_t pos,
                                     hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        if (l1 < l)
        {
            // do nothing
        }

        if (r < l) return;

        if (l == r)
        {
            size_t length = keys[l].size() << 3;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            collectHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;

            collectHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);

        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            collectHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            collectHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }

    struct NoPrefixKeySetData
    {
        size_t id;
        size_t max_link_length = 0;
        std::vector<std::pair<size_t, size_t>> link_lengths[2];
        std::vector<std::pair<size_t, bitstring>> link_chunks[2][32];
        std::vector<std::pair<size_t, bitstring>> is_leaf;
        hash_t* hashes;
    };

    inline void collectDataAndHashes(NoPrefixKeySetData& key_set_data, const std::vector<std::string>& keys,
                                     size_t l, size_t r, size_t pos,
                                     hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        if (l1 < l)
        {
            // do nothing
        }

        auto& is_leaf = key_set_data.is_leaf.emplace_back(id, bitstring::select(1));

        if (r < l)
        {
            is_leaf.second.set(0, false);
            return;
        }

        is_leaf.second.set(0, true);

        if (l == r)
        {
            size_t length = keys[l].size() << 3;

            size_t link_length = length - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);
            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);

            size_t link_length = next_pos - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);

        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[0].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[0], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[1].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[1], keys[m + 1], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }

    inline void collectHashes(NoPrefixKeySetData& key_set_data, const std::vector<std::string>& keys,
                              size_t l, size_t r, size_t pos,
                              hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        if (l1 < l)
        {
            // do nothing
        }

        if (r < l) return;

        if (l == r)
        {
            size_t length = keys[l].size() << 3;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            collectHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;

            collectHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);

        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            collectHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            collectHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }


    struct CommonPrefixKeySetData
    {
        size_t id = 0;
        size_t max_link_length = 0;
        std::vector<std::pair<size_t, size_t>> link_lengths[2];
        std::vector<std::pair<size_t, bitstring>> link_chunks[2][32];
        std::vector<std::pair<size_t, bitstring>> link_mask;
        std::vector<std::pair<size_t, bitstring>> is_endpoint;
        hash_t* hashes = 0;
    };

    inline void collectDataAndHashes(CommonPrefixKeySetData& key_set_data, const std::vector<std::string>& keys,
                                     size_t l, size_t r, size_t pos,
                                     hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        bool endpoint = false;
        if (l1 < l)
        {
            endpoint = true;
        }

        auto& mask = key_set_data.link_mask.emplace_back(id, bitstring::select(2));

        if (r < l)
        {
            return;
        }

        if (l == r)
        {
            size_t length = keys[l].size() << 3;

            size_t link_length = length - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            mask.second.set(bit, true);

            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);
            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);

            size_t link_length = next_pos - pos;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;

            mask.second.set(bit, true);

            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[bit].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[bit], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);
        auto& endp = key_set_data.is_endpoint.emplace_back(id, bitstring::select(1));

        endp.second.set(0, endpoint);
        mask.second.set(0, true);
        mask.second.set(1, true);
        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[0].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[0], keys[l], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            size_t link_length = next_pos - pos;
            link_length--;
            key_set_data.max_link_length = std::max(key_set_data.max_link_length, link_length);
            key_set_data.link_lengths[1].emplace_back(id, link_length);

            consumeLink(key_set_data.link_chunks[1], keys[m + 1], pos + 1, link_length, id);
            collectDataAndHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }

    inline void collectHashes(CommonPrefixKeySetData& key_set_data, const std::vector<std::string>& keys,
                              size_t l, size_t r, size_t pos,
                              hash_t cur_hash, hash_t seed)
    {
        size_t id = key_set_data.id++;

        key_set_data.hashes[id] = cur_hash;

        hash_t seeds[3];
        seeds[0] = nextRand(seed);
        seeds[1] = nextRand(seeds[0]);
        seeds[2] = nextRand(seeds[1]);
        hash_t child_hash[2] = {cur_hash ^ seeds[1], cur_hash ^ seeds[2] };

        size_t l1 = l;

        while (l <= r && keys[l].size() * 8 == pos) l++;

        if (l1 < l)
        {
            // do nothing
        }

        if (r < l) return;

        if (l == r)
        {
            size_t length = keys[l].size() << 3;
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;
            collectHashes(key_set_data, keys, l + 1, r, length, child_hash[bit], seeds[2]);
            return;
        }

        if (EXTRACT_BIT(keys[l], pos) == EXTRACT_BIT(keys[r], pos))
        {
            auto next_pos = findCommonPrefix(keys[l], keys[r], pos);
            size_t bit_id = pos & 7;
            size_t byte_id = pos >> 3;
            bool bit = (rev_bit[keys[l][byte_id]] >> bit_id) & 1;

            collectHashes(key_set_data, keys, l, r, next_pos, child_hash[bit], seeds[2]);
            return;
        }

        size_t m = split(keys, pos, l, r);

        {
            auto next_pos = findCommonPrefix(keys[l], keys[m], pos);
            collectHashes(key_set_data, keys, l, m, next_pos, child_hash[0], seeds[2]);
        }
        {
            auto next_pos = findCommonPrefix(keys[m + 1], keys[r], pos);
            collectHashes(key_set_data, keys, m + 1, r, next_pos, child_hash[1], seeds[2]);
        }
    }

    struct KeySetInfo
    {
        int type;
        size_t total_size;
        size_t min_size;
        size_t max_size;
    };

    inline KeySetInfo check(const std::vector<std::string>& keys)
	{
		assert(!keys.empty());

		size_t min_size = keys[0].size();
		size_t max_size = keys[0].size();
        int type = 0;
        size_t total_size = keys[0].size();
        for (size_t i = 1; i < keys.size(); ++i)
		{
			min_size = std::min(min_size, keys[i].size());
			max_size = std::max(max_size, keys[i].size());
            total_size += keys[i].size();
			if (type != 2 && keys[i - 1].size() < keys[i].size())
			{
				auto res = std::mismatch(keys[i - 1].begin(), keys[i - 1].end(), keys[i].begin());
				if (res.first == keys[i - 1].end())
				{
                    type = 2;
				}
			}
		}
        if (type != 2)
        {
            type = min_size != max_size;
        }
        return { type, total_size, min_size, max_size };
	}


	inline int compareEndpoints(const std::string& l, const std::string& r)
	{
		size_t len = std::min(l.size(), r.size());
		for (size_t it = 0; it < len; it++)
		{
			if (l[it] != r[it])
			{
				if ((uint8_t)l[it] < (uint8_t)r[it])
				{
					return -1;
				}
				else
				{
					return 1;
				}
			}
		}
		if (l.size() == r.size()) return 0;
		if (l.size() < r.size()) return -1;
		return 1;
	}
}

#endif
