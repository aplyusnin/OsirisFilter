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

#ifndef OSIRIS_COMMON_FILTER_H
#define OSIRIS_COMMON_FILTER_H

#include "osiris_filter.h"

namespace osiris
{
	class CommonFilter : public OsirisFilter
	{
		Dictionary* mask_storage = nullptr;
		Dictionary* endpoint_storage = nullptr;

        size_t construct(const std::vector<std::string>& keys, KeySetInfo& info)
        {
            size_t cnt = keys.size();
            hash_t* hashes = new hash_t[2 * cnt + 5];
            bitstring::init(1.2 * info.total_size);
            CommonPrefixKeySetData data;
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

            endpoint_storage = initDictionary(data.is_endpoint.size(), 1);
            built &= endpoint_storage->build(hashes, data.is_endpoint);

            mask_storage = initDictionary(data.link_mask.size(), 2);
            built &= mask_storage->build(hashes, data.link_mask);

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
                        }
                    }
                    built &= length[i]->build(hashes, data.link_lengths[i]);
                }

                built &= endpoint_storage->build(hashes, data.is_endpoint);
                built &= mask_storage->build(hashes, data.link_mask);
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

		bool pointQueryInternal(const std::string& key, uint8_t* link_buffer)
		{
			uint8_t mask = 0;
			uint8_t is_endpoint = 0;

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
			uint8_t bit;
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

				// check the state of the node
				this->mask_storage->get(cur, &mask);

				// if there is no link starting with the bit ---> no such key in the set
				if (!(mask & (1 << bit)))
				{
					return false;
				}

				// restoring next link in the set
				link_len = extractLink(bit, cur, link_buffer);
				// updating hash to point to the next trie's node
				UPDATE_HASH(cur, s, bit, hash_id, h1, h2)
				
				// since we don't keep the first bit of the link ---> go to the next bit
				NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
			}
			// if traversal ended in the middle of the link ---> there is no such key
			if (pt < link_len)
			{
				return false;
			}

			// we finished in some node, get it's state
			this->mask_storage->get(cur, &mask);
			// if it is a leaf or a key's end ---> the key exists
			if (mask != 3)
			{
				return true;
			}

			//otherwise check if there is a key ending in the vertex
			this->endpoint_storage->get(cur, &is_endpoint);
			return is_endpoint;
		}

		bool prefixQueryInternal(const std::string& key, uint8_t* link_buffer)
		{
			uint8_t mask = 0;
			uint8_t is_endpoint = 0;

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
			uint8_t bit;
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

				// check the state of the node
				this->mask_storage->get(cur, &mask);

				// if there is no link starting with the bit ---> no such key in the set
				if (!(mask & (1 << bit)))
				{
					return false;
				}

				// restoring next link in the set
				link_len = extractLink(bit, cur, link_buffer);
				// updating hash to point to the next trie's node
				UPDATE_HASH(cur, s, bit, hash_id, h1, h2)

				// since we don't keep the first bit of the link ---> go to the next bit
				NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
			}
			return true;
		}

		bool rangeQueryInternal(
			const std::string& left, bool include_left,
			const std::string& right, bool include_right,
			uint8_t* prefix_buffer, uint8_t* tail_buffer)
		{
			size_t left_key_size = left.size();

			size_t pos = 0;
			hash_t cur = hash_seed;
			hash_t s = hash_seed;
			uint8_t mask = 0;
			uint8_t is_endpoint = 0;

			size_t pt = 0;
			size_t link_len = 0;
			int hash_id = 0;

			// first bit of the left key
			bool left_bit;
			// first bit of the right key
			bool right_bit;
			hash_t h1, h2;

			uint8_t m0 = 128, m1 = 1;
			uint8_t left_value = left[0], right_value = right[0];
			uint8_t current_value = 0;

			// while the common part is not consumed. The longest common part is at most the left endpoint
			while (pos < left_key_size)
			{
				// extract next left bit
				left_bit = (left_value & m0) > 0;
				// extract next right bit
				right_bit = (right_value & m0) > 0;

				// if there is still a link, try to consume it
				if (pt < link_len)
				{
					// next byte is reached
					if (!(pt & 7))
					{
						current_value = prefix_buffer[pt >> 3];
						m1 = 1;
					}
					// extract current bit of the link
					bool current_bit = (current_value & m1) > 0;

					// if bits of the left and the right keys differ
					if (left_bit != right_bit)
					{
						if (current_bit == left_bit && rangeQueryLeftLink(left, pos, m0, pt, m1, link_len, cur, s, hash_id, prefix_buffer, tail_buffer, include_left)) return true;
						if (current_bit == right_bit && rangeQueryRightLink(right, pos, m0, pt, m1, link_len, cur, s, hash_id, prefix_buffer, tail_buffer, include_right)) return true;
						return false;
					}
					else
					{
						// if bits are equal
						// if they don't extend the link ---> no key  
						if (current_bit != left_bit)
						{
							return false;
						}

						m1 <<= 1;
						pt++;
						if (pt == link_len)
						{
							pt = link_len = 0;
						}
						// prepare to pick next bits form both keys
						NEXT_BITS_IN_LOOP(left, left_value, right, right_value, pos, m0, left_key_size)
					}
					continue;
				}

				// extract state of the node
				this->mask_storage->get(cur, &mask);

				// there is a split at the current node
				if (left_bit != right_bit)
				{
					// if there is a left link ---> check it
					if ((mask & 1) && rangeQueryTail(left, pos, m0, cur, s, hash_id, include_left, true, false, tail_buffer))
					{
						return true;
					}
					// if there is a right link ---> check it
					if ((mask & 2) && rangeQueryTail(right, pos, m0, cur, s, hash_id, include_right, false, false, tail_buffer))
					{
						return true;
					}
					return false;
				}

				// check if there is no way to extend the key
				if (!(mask & (1 << (int32_t)left_bit)))
				{
					return false;
				}

				// otherwise update link
				link_len = extractLink(left_bit, cur, prefix_buffer);
				pt = 0;

				// go to the next vertex
				UPDATE_HASH(cur, s, left_bit, hash_id, h1, h2)
			
				// prepare to get next bits
				NEXT_BITS_IN_LOOP(left, left_value, right, right_value, pos, m0, left_key_size)
			}

			// the only case ---> the left endpoint is a prefix of the right one

			// if left endpoint can be in the range ---> need to check if it exists
			if (include_left)
			{
				mask_storage->get(cur, &mask);
				// there is a key-end for sure
				if (mask != 3)
				{
					return true;
				}
				// otherwise need to check existence of the key
				endpoint_storage->get(cur, &is_endpoint);
				if (is_endpoint)
				{
					return true;
				}
			}

			// if we haven't stopped inside the link ---> traverse the right's key tail
			if (pt == link_len)
			{
				return rangeQueryTail(right, pos, m0, cur, s, hash_id, include_right, false, false, tail_buffer);
			}
			// otherwise try to consume the link first
			return rangeQueryRightLink(right, pos, m0, pt, m1, link_len, cur, s, hash_id, prefix_buffer, tail_buffer, include_right);
		}

		bool rangeQueryTail(
			const std::string& key,
			size_t pos, uint8_t m0,
			hash_t cur, hash_t current_seed, int hash_id,
			bool include_tail, bool isLeft,
			bool can_pick, uint8_t* tail_buffer) {

			size_t key_len = key.size();

			uint8_t val = key[pos];
			bool bit;

			uint8_t current_value = 0;
			bool current_bit;

			size_t pt = 0, link_len = 0;
			uint8_t m1 = 0;
			hash_t h1, h2;

			uint8_t mask;
			uint8_t is_endpoint;

			// Trying to traverse endpoint
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
					if (isLeft)
					{
						// if the link leads to the bigger key ---> there is a key in the segment 
						if (bit < current_bit)
							return true;
						// if it leads to the lower keys ---> no keys in the segment
						if (current_bit < bit)
							return false;
					}
					else
					{
						// vise versa for right endpoint
						if (current_bit < bit)
							return true;
						if (bit < current_bit)
							return false;
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

				this->mask_storage->get(cur, &mask);

				// if we are not on the common prefix
				if (can_pick)
				{
					// if we traverse right endpoint ---> it's prefixes belong to the segment
					if (!isLeft)
					{
						// if we are in a leaf or a key's end ---> it belongs to the segment
						if (mask != 3)
						{
							return true;
						}
						else
						{
							// otherwise check if there is a key ends in the node
							this->endpoint_storage->get(cur, &is_endpoint);
							if (is_endpoint)
								return true;
						}

					}
					// check for internal subsegments 
					if (isLeft != bit && mask & (1 << (1 - bit)))
						return true;
				}


				// otherwise we have to traverse further
				// if there is no links ---> failed to find any key
				if (!(mask & (1 << (int32_t)bit)))
					return false;

				// restoring next link
				link_len = extractLink(bit, cur, tail_buffer);
				// moving to the next node
				UPDATE_HASH(cur, current_seed, bit, hash_id, h1, h2)

					// we are not on the common prefix anymore
					can_pick = true;
				// preparing to pick next bit
				NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
			}

			// we traversed the endpoint

			// if there is a remaining link ---> it leads to the greater key
			if (pt < link_len)
			{
				return isLeft;
			}

			// otherwise we've finished in the node

			// if we traversed left endpoint ---> check if it belongs to the set
			if (isLeft)
			{
				mask_storage->get(cur, &mask);
				if (mask != 0)
				{
					return true;
				}
				else
				{
					return include_tail && can_pick;
				}
			}
			else
			{
				if (!can_pick) return false;
				mask_storage->get(cur, &mask);
				if (mask != 3)
				{
					return include_tail;
				}
				else
				{
					endpoint_storage->get(cur, &is_endpoint);
					return is_endpoint;
				}
			}
		}


		bool rangeQueryLeftLink(
			const std::string& left,
			size_t pos, uint8_t m0,
			size_t pos1, uint8_t m1, size_t len,
			hash_t cur, hash_t s, int hash_id,
			uint8_t* prefix_buffer, uint8_t* tail_buffer,
			bool include_left)
		{
			size_t key_len = left.size();

			uint8_t val = left[pos];
			uint8_t bit;

			uint8_t val1 = prefix_buffer[pos1 >> 3];
			uint8_t bit1;
			uint8_t mask;
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

			// left endpoint is fully traversed
			if (pos == key_len)
			{
				// no more link left
				if (pos1 == len)
				{
					this->mask_storage->get(cur, &mask);
					// there is a key greater than left endpoint
					if (mask != 0) return true;

					// if the endpoint belongs to the segment
					if (include_left) return include_left;
				}
				// there we can reach keys that greater than left endpoint
				return true;
			}
			// we still have some un-traversed part of the endpoint
			return rangeQueryTail(left, pos, m0, cur, s, hash_id, include_left, true, true, tail_buffer);
		}


		bool rangeQueryRightLink(
			const std::string& right,
			size_t pos, uint8_t m0,
			size_t pos1, uint8_t m1, size_t len,
			hash_t cur, hash_t s, int hash_id,
			const uint8_t* prefix_buffer, uint8_t* tail_buffer,
			bool include_right)
		{
			size_t key_length = right.size();

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
				NEXT_BIT_IN_LOOP(right, pos, val, m0, key_length)
			}


			uint8_t mask;
			uint8_t is_endpoint;
			// right endpoint is fully traversed
			if (pos == key_length)
			{
				// no more link left
				if (pos1 == len)
				{
					// only endpoint can belong to the segment
					if (include_right)
					{
						this->mask_storage->get(cur, &mask);
						if (mask != 3) return true;
						this->endpoint_storage->get(cur, &is_endpoint);
						return is_endpoint;
					}
				}
				// there we can reach only greater keys
				return false;
			}

			return rangeQueryTail(right, pos, m0, cur, s, hash_id, include_right, false, true, tail_buffer);
		}


		//////////////////////
		/// Serialization ///
		/////////////////////

		uint8_t getFilterId() override
		{
			return 3;
		}

		uint8_t* serializeExtra(uint8_t* buf) override
		{
			buf = mask_storage->serialize(buf);
			buf = endpoint_storage->serialize(buf);
			return buf;
		}

		size_t serializeExtraSize() override
		{
			return mask_storage->getSerializationSize() + endpoint_storage->getSerializationSize();
		}

	public:
		CommonFilter(const std::vector<std::string>& keys, KeySetInfo info)
		{
            auto retries = construct(keys, info);
            OSIRIS_DEBUG_PRINT("retries: ", retries);
		}

		CommonFilter(uint8_t* buf)
		{
			// assert(buf[0] == getFilterId());
			buf = deserializeCore(buf);
			auto [dict1, buf1] = deserializeDictionary(buf);

			mask_storage = dict1;
			buf = buf1;

			auto [dict2, buf2] = deserializeDictionary(buf);

			endpoint_storage = dict2;
		}

		~CommonFilter() override
		{
			delete mask_storage;
			delete endpoint_storage;
		}
	};
}
#endif