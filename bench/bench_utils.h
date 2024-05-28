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

#ifndef OSIRIS_BENCH_UTILS_H
#define OSIRIS_BENCH_UTILS_H

#include <string>
#include <vector>
#include <set>
#include <random>
#include <chrono>
#include "../include/osiris.h"

// setting specific hash_seed to reproduce the same data for testing
static std::mt19937 rngBench(100);

inline size_t rndNext(size_t l, size_t r)
{
    size_t val = rngBench() % (r - l + 1);
    return val + l;
}

struct cmp {
    bool operator() (const std::string& a, const std::string& b) const {
        size_t n = a.size();
        size_t m = b.size();
        for (size_t i = 0; i < std::min(n, m); i++)
        {
            if ((uint8_t)a[i] != (uint8_t)b[i])
            {
                return (uint8_t)a[i] < (uint8_t)b[i];
            }
        }
        return n < m;
    }
};

struct rangeQuery {
    std::string left;
    std::string right;
    bool includeLeft = false;
    bool includeRight = false;
    bool expectedResult = false;
};

struct testDataPoint
{
    std::vector<std::string> data;

    std::vector<std::string> pointQueries;
    std::vector<bool> pointResult;
};

struct testDataRange
{
    std::vector<std::string> data;

    std::vector<rangeQuery> rangeQueries;
};

testDataPoint preparePointTest(size_t size, size_t minLen, size_t maxLen, int setType, int type);

testDataRange prepareRangeTest(size_t size, size_t minLen, size_t maxLen, int setType, int type);

std::set<std::string, cmp> generateData(size_t size, size_t minLen, size_t maxLen, int setType);

struct testResult {
    std::string id;
    std::string name;
    size_t keysNum = 0;
    std::vector<long long> buildTime;

    std::vector<long long> serializationTime;
    std::vector<long long> deserializationTime;

    std::vector<long long> queryTime;
    size_t success = 0;
};

void saveQueryReport(std::vector<testResult>& result, const std::string& filename);

void saveBuildReport(std::vector<testResult>& result, const std::string& filename);

void saveSerialReport(std::vector<testResult>& result, const std::string& filename);

testResult evaluatePoint(testDataPoint* testData, size_t repeat, std::string id, std::string name, bool verify = true);

testResult evaluateRange(testDataRange* testData, size_t repeat, std::string id, std::string name, bool verify = true);
#endif //OSIRISFILTER_BENCH_UTILS_H
