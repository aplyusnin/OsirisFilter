# Osiris Filter

This is my master thesis project.

Osiris is a deterministic data structure for performing range emptiness queries over keyset. It is based on compact
storing of a keyset trie using key-values stores, that don't store keys explicitly.

It is headers-only library, to use just include it:
```c++
    #include "/include/osiris.h"
```

## How to use

To use it, prepare your keyset as a sorted std::vector of std::strings.

### Building 

Note: The keyset must be ordered in lexicographical order, where every character is unsigned.

```c++
    std::vector<std::string> keyset = ...; // key set to work with

    OsirisFitler* filter = osiris::build(keyset);
```

### Queries

Osiris supports 3 types of query:

- Point query (check if the key exists) 
- Prefix query (check if there is a key with the given prefix)
- Range query (check if there is a key within the given range)

For performing point query use:

```c++
    std::string key = ...; // key to search for
    bool result = filter->pointQuery(key); // checks if the key exists
```

Result variable stores true iff the key belongs to the keyset.

For performing prefix query use:

```c++
    std::string key = ...; // key to search for
    bool result = filter->prefixQuery(key); // checks if the key exists
```

For performing range query use:

```c++
    std::string left = ...;
    std::string right = ...;
    bool result = filter->rangeQuery(left, includeLeft, right, includeRight);
```

Result variable stores true iff there is any key in the keyset that belongs to the range. includeLeft and includeRight flags help to make segment closed or open.

### Serialization 

For serialization and deserialization use:

```c++
    auto data = filter->serialize();
```

It returns pointer to byte array, where the structure is stored and size of the array.

For deserialization use:

```c++
    uint8_t* data = ...; // restore serialized structure
    auto filter = osiris::deserialize(data); // deserialize it to the class 
```

### Examples and benchmarks

More examples can be found [here](examples.cpp).

To do some performance tests use: `bench/`

## Options

To run filter with debug log, compile with flag `OSIRIS_ENABLE_DEBUG`.


## License

This project is licensed under the GPLv3 License - see the [LICENSE](LICENSE) file for details.
