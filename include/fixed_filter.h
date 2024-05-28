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

#ifndef OSIRIS_FIXED_FILTER_H
#define OSIRIS_FIXED_FILTER_H

#include "osiris_filter.h"

namespace osiris
{
	class FixedLengthFilter : public OsirisFilter
	{
        //assume links with length more than 2^32 do not exist
		uint32_t key_length;
		uint8_t root_mask;

        //////////////////////
        /// Construction ///
        /////////////////////

        size_t construct(const std::vector<std::string>& keys, KeySetInfo& info)
        {
            size_t cnt = keys.size();
            hash_t* hashes = new hash_t[2 * cnt + 5];
            bitstring::init(1.2 * info.total_size);
            FixedKeySetData data;
            data.hashes = hashes;

            for (int i = 0; i < 2; ++i)
            {
                for (int j = 0; j < 32; ++j)
                {
                    data.link_chunks[i][j].reserve(cnt + 5);
                }
                data.link_lengths[i].reserve(cnt + 5);
            }

            bool built = false;

            data.id = 0;
            hash_seed = osiris_rng();

            collectDataAndHashes(data, keys, 0ull, keys.size() - 1, 0ull, hash_seed, hash_seed);

            max_link_size_in_bits = data.max_link_length;

            size_t length_bits = getSize(data.max_link_length);

            for (int i = 0; i < 2; ++i)
            {
                for (int j = 0; j < 32; ++j)
                {
                    if (!data.link_chunks[i][j].empty())
                    {
                        links_mask[i] |= (1ull << j);

                        links[i][j] = initDictionary(data.link_chunks[i][j].size(), 1ull << j);
                        built &= links[i][j]->build(hashes, data.link_chunks[i][j]);
                    }
                }
                length[i] = initDictionary(data.link_lengths[i].size(), length_bits);
                built &= length[i]->build(hashes, data.link_lengths[i]);
            }
            size_t retries = 0;
            while (!built)
            {
                retries++;
                hash_seed = osiris_rng();
                data.id = 0;
                collectHashes(data, keys, 0ull, keys.size() - 1, 0ull, hash_seed, hash_seed);
                built = true;
                for (int i = 0; i < 2; ++i)
                {
                    for (int j = 0; j < 32; ++j)
                    {
                        if (!data.link_chunks[i][j].empty())
                        {
                            built &= links[i][j]->build(hashes, data.link_chunks[i][j]);
                            if (!built) goto FAIL;
                        }
                    }
                    built &= length[i]->build(hashes, data.link_lengths[i]);
                    if (!built) goto FAIL;
                }
            FAIL:;
            }

            hash_cache[0] = nextRand(hash_seed);
            for (int i = 1; i < OSIRIS_HASH_CACHE_SIZE; ++i)
            {
                hash_cache[i] = nextRand(hash_cache[i - 1]);
            }
            bitstring::clear();
            delete[] hashes;
            return retries;
        }


        ////////////////
        /// Queries ///
        //////////////

		bool pointQueryInternal(const std::string& key, uint8_t* link_buffer) override
		{
			// if key has wrong size ---> it does not belong to the set
			if (key.size() != key_length) return false;

			// get the first bit of the key
			uint8_t bit = (key[0] >> 7) & 1;

			// if there is no keys starting with the first bit ---> no key in the set
			if (!((root_mask >> bit) & 1))
			{
				return false;
			}

			size_t pos = 0;
			size_t key_len = key.size();
			int hash_id = 0;
			hash_t cur = hash_seed;
			hash_t s = hash_seed;

			size_t link_len = 0;
			size_t pt = 0;

			uint8_t m0 = 128;
			uint8_t m1 = 0;

			uint8_t val = key[pos];
			uint8_t val1 = 0;
			uint8_t bit1;
			hash_t h1, h2;
			// try to locate the whole key in the trie
			while (pos < key_len)
			{
				// pick the next bit
				bit = (val & m0) > 0;

				// if there is still a link, try to consume it
				if (pt < link_len)
				{
					// if we reached the next byte of the link, update it
					if (!(pt & 7))
					{
						val1 = link_buffer[pt >> 3];
						m1 = 1;
					}
					// pick next bit of the link
					bit1 = (val1 & m1) > 0;

					// if bits differ ---> the key is not in the set
					if (bit != bit1)
					{
						return false;
					}
					m1 <<= 1;
					pt++;
					// if whole link was consumed --> forget it
					if (pt == link_len)
					{
						pt = link_len = 0;
					}
					// preparing to extract the next bit of the key
					NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
						continue;
				}
				// restoring next link in the set
				link_len = extractLink(bit, cur, link_buffer);
				// updating hash to point to the next trie's node
				UPDATE_HASH(cur, s, bit, hash_id, h1, h2)

					// since we don't keep the first bit of the link ---> go to the next bit
					NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
			}
			// if the whole key is traversed ---> key exists
			return true;
		}

		bool prefixQueryInternal(const std::string& key, uint8_t* link_buffer) override
		{
			// get the first bit of the key
			uint8_t bit = (key[0] >> 7) & 1;

			// if there is no keys starting with the first bit ---> no key in the set
			if (!((root_mask >> bit) & 1))
			{
				return false;
			}

			size_t pos = 0;
			size_t key_len = key.size();
			int hash_id = 0;
			hash_t cur = hash_seed;
			hash_t s = hash_seed;

			size_t link_len = 0;
			size_t pt = 0;

			uint8_t m0 = 128;
			uint8_t m1 = 0;

			uint8_t val = key[pos];
			uint8_t val1 = 0;
			uint8_t bit1;
			hash_t h1, h2;
			// try to locate the whole key in the trie
			while (pos < key_len)
			{
				// pick the next bit
				bit = (val & m0) > 0;

				// if there is still a link, try to consume it
				if (pt < link_len)
				{
					// if we reached the next byte of the link, update it
					if (!(pt & 7))
					{
						val1 = link_buffer[pt >> 3];
						m1 = 1;
					}
					// pick next bit of the link
					bit1 = (val1 & m1) > 0;

					// if bits differ ---> the key is not in the set
					if (bit != bit1)
					{
						return false;
					}
					m1 <<= 1;
					pt++;
					// if whole link was consumed --> forget it
					if (pt == link_len)
					{
						pt = link_len = 0;
					}
					// preparing to extract the next bit of the key
					NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
						continue;
				}
				// restoring next link in the set
				link_len = extractLink(bit, cur, link_buffer);
				// updating hash to point to the next trie's node
				UPDATE_HASH(cur, s, bit, hash_id, h1, h2)

				// since we don't keep the first bit of the link ---> go to the next bit
                NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
			}
			// if the whole prefix is traversed ---> key exists
			return true;
		}

        bool rangeQueryInternal(
            const std::string& left, bool include_left,
            const std::string& right, bool include_right,
            uint8_t* prefix_buffer, uint8_t* tail_buffer)
        {
            uint32_t left_key_size = (uint32_t)left.size();
            uint32_t right_key_size = (uint32_t)right.size();

            // we have to traverse the common prefix but not longer than size of keys in the set
            uint32_t key_len = std::min({ key_length, left_key_size, right_key_size });

            uint32_t pos = 0;
            hash_t cur = hash_seed;
            hash_t s = hash_seed;

            uint32_t pt = 0;
            uint32_t linkLen = 0;
            int hashId = 0;

            // first bit of the left key
            bool leftBit = (left[0] >> 7) & 1;
            // first bit of the right key
            bool rightBit = (right[0] >> 7) & 1;
            hash_t h1, h2;

            // if first bits differ ---> we have to check existence of the keys >/>= than the left starting with 0
            // or keys </<= than right starting with 1
            if (leftBit != rightBit)
            {
                // if there is keys starting with 0 ---> check keys starting with 0
                if (root_mask & 1)
                    if (rangeQueryTail(left, 0, 128, s, s, 0, include_left, true, false, tail_buffer)) return true;
                // if there is keys starting with 1 ---> check keys starting with 1
                if (root_mask & 2)
                    if (rangeQueryTail(right, 0, 128, s, s, 0, include_right, false, false, tail_buffer)) return true;
                // no keys found
                return false;
            }

            // both the left and the right keys start with the same bit

            // check if there are keys starting with it
            if (!(root_mask >> (int32_t)leftBit))
            {
                return false;
            }

            uint8_t m0 = 128, m1 = 1;
            uint8_t leftValue = left[0], rightValue = right[0];
            uint8_t currentValue = 0;

            // while the common part is not consumed
            while (pos < key_length)
            {
                // extract next left bit
                leftBit = (leftValue & m0) > 0;
                // extract next right bit
                rightBit = (rightValue & m0) > 0;

                // if there is still a link, try to consume it
                if (pt < linkLen)
                {
                    // next byte is reached
                    if (!(pt & 7))
                    {
                        currentValue = prefix_buffer[pt >> 3];
                        m1 = 1;
                    }
                    // extract current bit of the link
                    bool currentBit = (currentValue & m1) > 0;
                    // if bits of the left and the right keys differ
                    if (leftBit != rightBit)
                    {
                        if (currentBit == leftBit && rangeQueryLeftLink(left, pos, m0, pt, m1, linkLen, cur, s, hashId, prefix_buffer, tail_buffer, include_left)) return true;
                        if (currentBit == rightBit && rangeQueryRightLink(right, pos, m0, pt, m1, linkLen, cur, s, hashId, prefix_buffer, tail_buffer, include_right)) return true;
                        return false;
                    }
                    // if bits are equal
                    // if they don't extend the link ---> no key
                    if (currentBit != leftBit)
                    {
                        return false;
                    }

                    m1 <<= 1;
                    pt++;
                    if (pt == linkLen)
                    {
                        pt = linkLen = 0;
                    }
                    // prepare to pick next bits form both keys
                    NEXT_BITS_IN_LOOP(left, leftValue, right, rightValue, pos, m0, key_length)
                        continue;
                }

                // there is a split at the current node
                if (leftBit != rightBit)
                {
                    if (rangeQueryTail(left, pos, m0, cur, s, hashId, include_left, true, false, tail_buffer))
                    {
                        return true;
                    }
                    if (rangeQueryTail(right, pos, m0, cur, s, hashId, include_right, false, false, tail_buffer))
                    {
                        return true;
                    }
                    return false;
                }

                // otherwise update link
                linkLen = extractLink(leftBit, cur, prefix_buffer);
                pt = 0;

                // go to the next vertex
                UPDATE_HASH(cur, s, leftBit, hashId, h1, h2)
                // prepare to get next bits
                NEXT_BITS_IN_LOOP(left, leftValue, right, rightValue, pos, m0, key_length)
            }

            // if we didn't reach left keys size's end ---> there is no keys we're looking for
            if (pos != left_key_size)
            {
                return false;
            }

            // otherwise the left key is traversed
            // if the common prefix is in a leaf ---> it is the only key in the segment, check if it belongs to the segment
            if (pos == key_length)
            {
                return include_left;
            }

            // the only case ---> left key is a prefix of valid keys, trying to find them
            // if we haven't stopped inside the link ---> traverse the right's key tail
            if (pt == linkLen)
            {
                return rangeQueryTail(right, pos, m0, cur, s, hashId, include_right, false, true, tail_buffer);
            }
            // otherwise try to consume the link first
            return rangeQueryRightLink(right, pos, m0, pt, m1, linkLen, cur, s, hashId, prefix_buffer, tail_buffer, include_right);
        }


        bool rangeQueryTail(
            const std::string& key,
            size_t pos, uint8_t m0,
            hash_t cur, hash_t current_seed, int hash_id,
            bool include_tail, bool is_left,
            bool can_pick, uint8_t* tail_buffer) {

            // We cannot traverse further than the key_size
            uint32_t key_len = std::min((uint32_t)key.size(), key_length);

            uint8_t val = key[pos];
            bool bit;

            uint8_t current_value = 0;
            bool current_bit;

            size_t pt = 0, link_len = 0;
            uint8_t m1 = 0;
            hash_t h1, h2;

            // Until endpoint is not traversed (or leaf reached)
            while (pos < key_len)
            {
                // extract next bit
                bit = (val & m0) > 0;

                // if there is a link
                if (pt < link_len)
                {
                    // if we reached next byte
                    if (!(pt & 7))
                    {
                        current_value = tail_buffer[pt >> 3];
                        m1 = 1;
                    }
                    // pick next bit
                    current_bit = (current_value & m1) > 0;

                    // if we traverse left endpoint
                    if (is_left)
                    {
                        // if the link leads to the bigger key ---> there is a key in the segment
                        if (bit < current_bit)
                        {
                            return true;
                        }
                        // if it leads to the lower keys ---> no keys in the segment
                        if (current_bit < bit)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        // vise versa for right endpoint
                        if (current_bit < bit)
                        {
                            return true;
                        }
                        if (bit < current_bit)
                        {
                            return false;
                        }
                    }

                    m1 <<= 1;
                    pt++;
                    // if we consumed the link
                    if (pt == link_len)
                    {
                        pt = link_len = 0;
                    }
                    // we are not in the on the common prefix anymore ---> we can check if the node corresponds to some key
                    can_pick = true;

                    // prepare to pick next bit
                    NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
                    continue;
                }
                // if we're not on the common prefix ---> one of the following conditions holds:
                // 1) l < key
                // 2) key < r
                // For the first case isLeft flag set to false and there exists keys iff next bit is 1
                // The second case is similar: isLeft flag set to true and next bit should be 0
                // Therefore if isLeft flag differs from next bit ---> there is keys in the range
                if (can_pick && is_left != bit)
                    return true;

                // restoring next link
                link_len = extractLink(bit, cur, tail_buffer);
                // moving to the next node
                UPDATE_HASH(cur, current_seed, bit, hash_id, h1, h2)

                // preparing to pick next bit
                NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)

                // we are not on the common prefix anymore
                can_pick = true;
            }
            // if we traversed only prefix of the endpoint ---> the key we finished at only suits for the right endpoint
            if (pos < key.size()) return !is_left;

            //otherwise, endpoint belongs to the keyset ---> check if it belongs to the segment
            return include_tail;
        }

        bool rangeQueryLeftLink(
            const std::string& left,
            size_t pos, uint8_t m0,
            size_t pos1, uint8_t m1, size_t len,
            hash_t cur, hash_t s, int hash_id,
            uint8_t* prefix_buffer, uint8_t* tail_buffer,
            bool include_left)
        {
            uint32_t key_len = std::min((uint32_t)left.size(), key_length);

            uint8_t val = left[pos];
            uint8_t bit;

            uint8_t val1 = prefix_buffer[pos1 >> 3];
            uint8_t bit1;
            while (pos1 < len)
            {
                if (!(pos1 & 7))
                {
                    val1 = prefix_buffer[pos1 >> 3];
                    m1 = 1;
                }
                bit = (val & m0) > 0;
                bit1 = (val1 & m1) > 0;

                if (bit < bit1) return true;
                if (bit1 < bit) return false;

                pos1++;
                m1 <<= 1;
                NEXT_BIT_IN_LOOP(left, pos, val, m0, key_len)
            }

            if (pos == key_length) return pos == left.size() && include_left;
            else return rangeQueryTail(left, pos, m0, cur, s, hash_id, include_left, true, true, tail_buffer);
        }

        bool rangeQueryRightLink(
            const std::string& right,
            size_t pos, uint8_t m0,
            size_t pos1, uint8_t m1, size_t len,
            hash_t cur, hash_t s, int hashId,
            uint8_t* prefix_buffer, uint8_t* tail_buffer,
            bool include_right)
        {
            uint32_t key_len = std::min((uint32_t)right.size(), key_length);

            uint8_t val = right[pos];
            bool bit;

            uint8_t val1 = prefix_buffer[pos1 >> 3];
            bool bit1;
            while (pos1 < len)
            {
                if (!(pos1 & 7))
                {
                    val1 = prefix_buffer[pos1 >> 3];
                    m1 = 1;
                }
                bit = (val & m0) > 0;
                bit1 = (val1 & m1) > 0;

                if (bit1 < bit) return true;
                if (bit < bit1) return false;

                pos1++;
                m1 <<= 1;
                NEXT_BIT_IN_LOOP(right, pos, val, m0, key_len)
            }

            if (pos == key_length) return pos == right.size() && include_right;
            else return  rangeQueryTail(right, pos, m0, cur, s, hashId, include_right, false, true, tail_buffer);
        }

		//////////////////////
		/// Serialization ///
		/////////////////////

        uint8_t getFilterId() override
        {
            return 1;
        }

        uint8_t* serializeExtra(uint8_t* buf) override
        {
            memmove(buf, &root_mask, sizeof(root_mask));
            buf += sizeof(root_mask);
            memmove(buf, &key_length, sizeof(key_length));
            return buf + sizeof(key_length);
        }

        size_t serializeExtraSize() override
        {
            return sizeof(key_length) + sizeof(root_mask);
        }

	public:

		FixedLengthFilter(const std::vector<std::string>& keys, KeySetInfo info)
		{
            key_length = info.max_size;

            root_mask = 0;
            root_mask |= (1 << (((uint8_t)keys[0][0]) >> 7));
            root_mask |= (1 << (((uint8_t)keys.back()[0]) >> 7));

            auto retries = construct(keys, info);
            OSIRIS_DEBUG_PRINT("retries: ", retries);
		}

        // deserializing constructor 
		FixedLengthFilter(uint8_t* buf)
		{
            uint8_t* kekw = buf;
            // assert(buf[0] == getFilterId());
            buf = deserializeCore(buf);

            memmove(&root_mask, buf, sizeof(root_mask));
            buf += sizeof(root_mask);
            memmove(&key_length, buf, sizeof(key_length));
		}

        ~FixedLengthFilter() override = default;
	};
}

#endif