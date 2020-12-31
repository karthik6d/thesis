// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <map>

using namespace ROCKSDB_NAMESPACE;
using namespace std;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

enum query_type { read_query, write_query, update_query, delete_query };

typedef struct workload_entry {
  query_type type;
  string key;
  string value;
} workload_entry;

void load(string path, DB* db) {
  // Read data from CSV File
  string new_path = "../lsm_tree_compression/data.csv";
  ifstream data_file(new_path, ios::ate);

  int length = data_file.tellg();
  data_file.close();

  FILE* f = fopen(new_path.c_str(), "r");

  char* data =
      (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);
  fclose(f);

  string data_str(data, data + length);

  int i = 0;

  int lines = count(data_str.begin(), data_str.end(), '\n');

  cout << "reading csv";

  istringstream ss(data_str);

  string line;

  while (getline(ss, line)) {
    cout << "\r" << ++i << "/" << lines << flush;

    istringstream ss(line);

    string key_s, value_s;

    getline(ss, key_s, ',');
    getline(ss, value_s, ',');

    Status s = db->Put(WriteOptions(), key_s, value_s);
    assert(s.ok());
  }

  cout << endl;
}

vector<workload_entry> get_workload(string query_path, DB* db){
  string output_str;
  ifstream query_file(query_path);
  vector<workload_entry> workload;

  // Parse through workload file
  while (getline(query_file, output_str)) {
    istringstream ss(output_str);
    vector<string> elements;

    do {
      string word;
      ss >> word;
      elements.push_back(word);
    } while (ss);

    if (elements[0].compare("create") == 0) {
      cout << "Create" << endl;

    } else if (elements[0].compare("load") == 0) {
      cout << "Load" << endl;
      load(elements.at(1), db);

    } else if (elements[0].compare("read") == 0) {
      workload.push_back({read_query, elements[1], ""});

    } else if (elements[0].compare("write") == 0) {
      workload.push_back({write_query, elements[1], elements[2]});

    } else if (elements[0].compare("delete") == 0) {
      workload.push_back({delete_query, elements[1]});

    } else if (elements[0].compare("update") == 0) {
      workload.push_back({update_query, elements[1], elements[2]});

    } else {
      throw runtime_error("Unknown command");
    }
  }

  return workload;
}

int main(int argc, char** argv) {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // Map of integers to all possible compression schemes
  map<int, CompressionType> compression_types;
  compression_types[0] = kNoCompression;
  compression_types[1] = kSnappyCompression;
  compression_types[2] = kZlibCompression;
  compression_types[3] = kBZip2Compression;
  compression_types[4] = kLZ4Compression;
  compression_types[5] = kLZ4HCCompression;
  compression_types[6] = kXpressCompression;
  compression_types[7] = kZSTD;

  int compression_scheme;
  vector<CompressionType> compression_by_level;

  // Handle all the different compression options
  // Number Argument (Compression Mode): 1 - No Compression, 2 - Bottommost Compression, 8 - Level by Level Compression
  cout << "Number of arguments: " << argc << endl;
  switch(argc) {
    case 1: {
      //std::cout << "Here in no compression" << endl;
      options.compression = kNoCompression;
      //std::cout << "After defining compression";
      break;
    }

    case 2: {
      //std::cout<<"what up"<<endl;
      compression_scheme = atoi(argv[1]);
      options.bottommost_compression = compression_types[compression_scheme];
      break;
    }

    case 8: {
      compression_by_level.push_back(compression_types[atoi(argv[1])]);
      compression_by_level.push_back(compression_types[atoi(argv[2])]);
      compression_by_level.push_back(compression_types[atoi(argv[3])]);
      compression_by_level.push_back(compression_types[atoi(argv[4])]);
      compression_by_level.push_back(compression_types[atoi(argv[5])]);
      compression_by_level.push_back(compression_types[atoi(argv[6])]);
      compression_by_level.push_back(compression_types[atoi(argv[7])]);
      options.compression_per_level = compression_by_level;
      break;
    }
    
    default: { throw runtime_error("Invalid number of arguments"); }
  }


  //options.compression_per_level
  // options.compression = kNoCompression;
  // options.bottommost_compression = kNoCompression;

  std::cout << "After declaration" << endl;
  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  std::cout << "After declaration" << endl;
  // Get the workload from the query file from our data generator
  string path = "../lsm_tree_compression/queries.dsl";
  vector<workload_entry> new_workload = get_workload(path, db);
  vector<string> res;

  std::cout << "After declaration" << endl;
  clock_t start = clock();
  // Start the workload
  for (auto e : new_workload) {
    Status s;
    switch (e.type) {
      case read_query: {
        string value;
        // get value
        s = db->Get(ReadOptions(), e.key, &value);

        if (s.ok()) {
          res.push_back(value);
        }

        break;
      }

      case write_query: {
        s = db->Put(WriteOptions(), e.key, e.value);
        assert(s.ok());
        break;
      }

      case delete_query: {
        s = db->Put(WriteOptions(), "key1", "value");
        assert(s.ok());
        break;
      }

      case update_query: {
        s = db->Put(WriteOptions(), "key1", "value");
        assert(s.ok());
        break;
      }

      default: { throw runtime_error("Unsupported workload command"); }
    }
  }
  double duration = (clock() - start) / (double) CLOCKS_PER_SEC;
  std::cout << "Execution Time of Completing the workload: " << duration << endl;
  
  // Write all the reads to a file
  ofstream f("../lsm_tree_compression/hello.res");
  for (string r : res) {
    f << r << endl;
  }
  f.close();

  // Delete the database after the workload is completed
  delete db;
  return 0;
}