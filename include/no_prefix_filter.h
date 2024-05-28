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

#ifndef OSIRIS_NO_PREFIX_H
#define OSIRIS_NO_PREFIX_H

#include "osiris_filter.h"

namespace osiris
{
	class NoPrefixFilter : public OsirisFilter
	{
		Dictionary* leaf_masks = nullptr;
		uint8_t root_mask = 0;

        size_t construct(const std::vector<std::string>& keys, KeySetInfo& info)
        {
            size_t cnt = keys.size();
            hash_t* hashes = new hash_t[2 * cnt + 5];
            bitstring::init(1.2 * info.total_size);
            NoPrefixKeySetData data;
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

            leaf_masks = initDictionary(data.is_leaf.size(), 1);
            built &= leaf_masks->build(hashes, data.is_leaf);

            size_t retries = 0;

            while (!built)
            {
                retries ++;
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

                built &= leaf_masks->build(hashes, data.is_leaf);
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


        bool pointQueryInternal(const std::string& key, uint8_t* link_buffer) override
		{
			uint8_t current_mask = 0;

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

				// check the state of the node
				this->leaf_masks->get(cur, &current_mask);

				// if we reached a leaf
				if (current_mask == 0)
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
			this->leaf_masks->get(cur, &current_mask);
			// if it is a leaf ---> the key exists
			return current_mask == 0;
		}

		bool prefixQueryInternal(const std::string& key, uint8_t* link_buffer) override
		{
			uint8_t current_mask = 0;

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

				// check the state of the node
				this->leaf_masks->get(cur, &current_mask);

				// if we reached a leaf
				if (current_mask == 0)
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
			uint8_t is_leaf = 0;

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
				this->leaf_masks->get(cur, &is_leaf);

				// if we reached a leaf
				if (is_leaf == 0) return false;

				// there is a split at the current node
				if (left_bit != right_bit)
				{
					// check left link
					if (rangeQueryTail(left, pos, m0, cur, s, hash_id, include_left, true, false, tail_buffer))
					{
						return true;
					}
					//  check right link
					if (rangeQueryTail(right, pos, m0, cur, s, hash_id, include_right, false, false, tail_buffer))
					{
						return true;
					}
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

			// if we haven't stopped inside the link ---> traverse the right's key tail
			if (pt == link_len)
			{
				return rangeQueryTail(right, pos, m0, cur, s, hash_id, include_right, false, true, tail_buffer);
			}
			// otherwise try to consume the link first
			return rangeQueryRightLink(right, pos, m0, pt, m1, link_len, cur, s, hash_id, prefix_buffer, tail_buffer, include_right);
		}

		bool rangeQueryTail(
			const std::string& key,
			size_t pos, uint8_t m0,
			hash_t cur, hash_t current_seed, int hash_id,
			bool include_tail, bool is_left,
			bool can_pick, uint8_t* tail_buffer) {

			size_t key_len = key.size();

			uint8_t val = key[pos];
			bool bit;

			uint8_t current_value = 0;
			bool current_bit;

			size_t pt = 0, link_len = 0;
			uint8_t m1 = 0;
			hash_t h1, h2;

			uint8_t is_leaf;

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
					if (is_left)
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

				this->leaf_masks->get(cur, &is_leaf);


				// we reached a leaf
				if (is_leaf == 0)
				{
					// the key is less than endpoint ---> valid for right endpoint traversal 
					return can_pick && !is_left;
				}

				// otherwise the degree is 2
				// if the next bit is 0 for left endpoint or 1 for right endpoint --->  there exists a key in the segment
				if (bit != is_left && can_pick)
				{
					return true;
				}

				// restoring next link
				link_len = extractLink(bit, cur, tail_buffer);
				// moving to the next node
				UPDATE_HASH(cur, current_seed, bit, hash_id, h1, h2)

				// preparing to pick next bit
				NEXT_BIT_IN_LOOP(key, pos, val, m0, key_len)
				// we are not on the common prefix anymore
				can_pick = true;
			}

			leaf_masks->get(cur, &is_leaf);
			// if endpoint is a key in the set
			if (is_leaf == 0)
			{
				return include_tail;
			}
			else
			{
				// otherwise we have keys > than endpoint ---> valid for left endpoint
				return is_left;
			}
		}

		bool rangeQueryLeftLink(
			const std::string& left,
			size_t pos, uint8_t m0,
			size_t pos1, uint8_t m1, size_t len,
			hash_t cur, hash_t s, int hash_id,
			uint8_t* prefix_buffer, uint8_t* tail_buffer,
			bool includeLeft)
		{
			size_t key_len = left.size();

			uint8_t val = left[pos];
			uint8_t bit;

			uint8_t val1 = prefix_buffer[pos1 >> 3];
			uint8_t bit1;
			uint8_t meta_value;
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
					this->leaf_masks->get(cur, &meta_value);
					return meta_value || includeLeft;
				}
				// there we can reach keys that greater than left endpoint
				return true;
			}
			// we still have some un-traversed part of the endpoint
			return rangeQueryTail(left, pos, m0, cur, s, hash_id, includeLeft, true, true, tail_buffer);
		}


		bool rangeQueryRightLink(
			const std::string& right,
			size_t pos, uint8_t m0,
			size_t pos1, uint8_t m1, size_t len,
			hash_t cur, hash_t s, int hash_id,
			uint8_t* prefix_buffer, uint8_t* tail_buffer,
			bool include_right)
		{
			size_t key_len = right.size();

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

			// right endpoint is fully traversed
			if (pos == key_len)
			{
				// no more link left
				if (pos1 == len)
				{
					if (include_right)
					{
						uint8_t is_leaf;
						this->leaf_masks->get(cur, &is_leaf);
						return is_leaf == 0;
					}
					else
					{
						return false;
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
			return 2;
		}

		uint8_t* serializeExtra(uint8_t* buf) override
		{
			memmove(buf , &root_mask, sizeof(root_mask));
			buf += sizeof(root_mask);
			buf = leaf_masks->serialize(buf);
			return buf;
		}

		size_t serializeExtraSize() override
		{
			return leaf_masks->getSerializationSize() + sizeof(root_mask);
		}

	public:
		NoPrefixFilter(const std::vector<std::string>& keys, KeySetInfo info)
		{
            root_mask = 0;
            root_mask |= (1 << (((uint8_t)keys[0][0]) >> 7));
            root_mask |= (1 << (((uint8_t)keys.back()[0]) >> 7));

            auto retries = construct(keys, info);
            OSIRIS_DEBUG_PRINT("retries: ", retries);

        }

		NoPrefixFilter(uint8_t* buf)
		{
			// assert(buf[0] == getFilterId());
			buf = deserializeCore(buf);

			memmove(&root_mask, buf, sizeof(root_mask));
			buf += sizeof(root_mask);
			leaf_masks = deserializeDictionary(buf).first;
		}

		~NoPrefixFilter() override
		{
			delete leaf_masks;
		}
	};
}

#endif