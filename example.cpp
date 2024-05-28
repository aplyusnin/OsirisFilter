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

#include <iostream>
#include <cassert>
#include "include/osiris.h"

using namespace std;

void exampleFixedLength()
{
    vector<string> keys = { "ab", "ac", "bc" };

    auto filter = osiris::build(keys);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("b"));
    assert(!filter->prefixQuery("d"));

    assert(filter->pointQuery("ab"));
    assert(filter->pointQuery("ac"));
    assert(!filter->pointQuery("dc"));

    assert(filter->rangeQuery("a", true, "b", false));
    assert(filter->rangeQuery("ab", true, "ac", true));
    assert(!filter->rangeQuery("ab", false, "ac", false));

    auto data = filter->serialize();

    delete filter;

    filter = osiris::deserialize(data.first);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("b"));
    assert(!filter->prefixQuery("d"));

    assert(filter->pointQuery("ab"));
    assert(filter->pointQuery("ac"));
    assert(!filter->pointQuery("dc"));

    assert(filter->rangeQuery("a", true, "b", false));
    assert(filter->rangeQuery("ab", true, "ac", true));
    assert(!filter->rangeQuery("ab", false, "ac", false));

    delete filter;
    delete[] data.first;
}

void exampleNoPrefix()
{
    vector<string> keys = {"abc", "amogus", "kek"};

    auto filter = osiris::build(keys);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("am"));
    assert(!filter->prefixQuery("ac"));

    assert(filter->pointQuery("abc"));
    assert(filter->pointQuery("amogus"));
    assert(!filter->pointQuery("acab"));

    assert(filter->rangeQuery("abc", true, "am", false));
    assert(filter->rangeQuery("amogus", true, "amogus", true));
    assert(!filter->rangeQuery("abc", false, "amogus", false));

    auto data = filter->serialize();

    delete filter;

    filter = osiris::deserialize(data.first);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("am"));
    assert(!filter->prefixQuery("ac"));

    assert(filter->pointQuery("abc"));
    assert(filter->pointQuery("amogus"));
    assert(!filter->pointQuery("acab"));

    assert(filter->rangeQuery("abc", true, "am", false));
    assert(filter->rangeQuery("amogus", true, "amogus", true));
    assert(!filter->rangeQuery("abc", false, "amogus", false));

    delete filter;
    delete[] data.first;
}

void exampleCommon()
{
    vector<string> keys = {"abc", "amogus", "kek", "kekw"};

    auto filter = osiris::build(keys);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("am"));
    assert(!filter->prefixQuery("ac"));

    assert(filter->pointQuery("abc"));
    assert(filter->pointQuery("amogus"));
    assert(!filter->pointQuery("acab"));

    assert(filter->rangeQuery("abc", true, "am", false));
    assert(filter->rangeQuery("amogus", true, "amogus", true));
    assert(!filter->rangeQuery("abc", false, "amogus", false));

    auto data = filter->serialize();

    delete filter;

    filter = osiris::deserialize(data.first);

    assert(filter->prefixQuery("a"));
    assert(filter->prefixQuery("am"));
    assert(!filter->prefixQuery("ac"));

    assert(filter->pointQuery("abc"));
    assert(filter->pointQuery("amogus"));
    assert(!filter->pointQuery("acab"));

    assert(filter->rangeQuery("abc", true, "am", false));
    assert(filter->rangeQuery("amogus", true, "amogus", true));
    assert(!filter->rangeQuery("abc", false, "amogus", false));

    delete filter;
    delete[] data.first;
}

int main()
{
    // Note: since small key set is used ---> retries may be required
    exampleFixedLength();
    exampleNoPrefix();
    exampleCommon();

	return 0;
}