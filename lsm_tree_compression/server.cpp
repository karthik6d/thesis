#include "server.h"
#include "lsm_tree.h"
#include "constants.h"

#include <assert.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <unordered_map>
#include <string> 
#include <stdexcept>

using namespace std;

vector<workload_entry> workload;

void create(string db_name, int is_compressed, unordered_map<string, float> constants, unordered_map<string, dataset> models, bool read_only, bool write_only, float leniency) {
  LSM_Tree* db = new LSM_Tree();
  db->name = db_name;
  db->compressed = is_compressed;
  db->constants = constants;
  db->read_only = read_only;
  db->write_only = write_only;
  db->leniency = leniency;
  db->models = models;
  current_db = db;

  mkdir("data", 0755);
}

void load(string path) {
  // Read data from CSV File
  ifstream data_file(path, ios::ate);

  int length = data_file.tellg();
  data_file.close();

  FILE* f = fopen(path.c_str(), "r");

  char* data =
      (char*)mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fileno(f), 0);
  fclose(f);

  string data_str(data, data + length);

  // if(munmap(data, length) != 0){
  //   cout << strerror(errno) << endl;
  // }

  //int i = 0;

  //int lines = count(data_str.begin(), data_str.end(), '\n');

  cout << "reading csv" << endl;

  istringstream ss(data_str);

  string line;

  clock_t start;
  double duration;
  start = clock();  


  while (getline(ss, line)) {
    // cout << "\r" << ++i << "/" << lines << flush;

    istringstream ss(line);

    string key_s, value_s;

    getline(ss, key_s, ',');
    getline(ss, value_s, ',');

    current_db->write(stoi(key_s), stoi(value_s));
  }
  duration = (clock() - start) / (double) CLOCKS_PER_SEC;
  cout << "Execution Time of Load: " << duration << endl;
  cout << endl;
}

int main(int argc, char** argv) {
  if (argc != 6) {
    std::cout << "Need to pass in query file or compressed state" << endl;
    return 1;
  }

  string output_str;
  int is_compressed = atoi(argv[1]);
  ifstream query_file(argv[2]);
  bool read_only = atoi(argv[3]);
  bool write_only = atoi(argv[4]);
  float leniency = stof(argv[5]);

  unordered_map<string, float> constants = write_constants_to_file();
  vector<string> training_data_paths;
  training_data_paths.push_back("../data_sketching/clean_data/clean_snappy.csv");
  training_data_paths.push_back("../data_sketching/clean_data/clean_simd.csv");
  training_data_paths.push_back("../data_sketching/clean_data/clean_rle.csv");
  training_data_paths.push_back("../data_sketching/clean_data/clean_zlib.csv");
  training_data_paths.push_back("../data_sketching/clean_data/clean_zstandard.csv");

  vector<int> training_data_num_features;
  training_data_num_features.push_back(770);
  training_data_num_features.push_back(770);
  training_data_num_features.push_back(770);
  training_data_num_features.push_back(769);
  training_data_num_features.push_back(770);

  unordered_map<string, dataset> rf_models;
  rf_models["snappy"] = get_training_data(training_data_paths.at(0), 770, 50);
  rf_models["simd"] = get_training_data(training_data_paths.at(1), 770, 50);
  rf_models["rle"] = get_training_data(training_data_paths.at(2), 770, 50);
  rf_models["zlib"] = get_training_data(training_data_paths.at(3), 769, 50);
  rf_models["zstandard"] = get_training_data(training_data_paths.at(4), 770, 50);

  clock_t start;
  double duration;

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
      create(elements.at(1), is_compressed, constants, rf_models, read_only, write_only, leniency);
    } else if (elements[0].compare("load") == 0) {
      load(elements.at(1));
    } else if (elements[0].compare("read") == 0) {
      workload.push_back({read_query, stoi(elements[1]), 0});

    } else if (elements[0].compare("write") == 0) {
      workload.push_back({write_query, stoi(elements[1]), stoi(elements[2])});

    } else if (elements[0].compare("delete") == 0) {
      workload.push_back({delete_query, stoi(elements[1])});

    } else if (elements[0].compare("update") == 0) {
      workload.push_back({update_query, stoi(elements[1]), stoi(elements[2])});

    } else {
      throw runtime_error("Unknown command");
    }
  }

  cout << "starting workload" << endl;
  start = clock();
  
  auto result = execute_workload();

  duration = (clock() - start) / (double) CLOCKS_PER_SEC;
  cout << "Execution Time of Reads and Writes: " << duration << endl;
  cout << "finished workload, writing results to file" << endl;

  ofstream f("hello.res");

  for (int r : result) {
    f << r << endl;
  }

  f.close();

  return 0;
}

vector<int> execute_workload() {
  vector<int> res;

  int i = 0;
  for (auto e : workload) {
    i++;

    switch (e.type) {
      case read_query: {
        pair<read_result, int> r = current_db->read(e.key);

        if (r.first == found) {
          res.push_back(r.second);
        }

        break;
      }

      case write_query: {
        current_db->write(e.key, e.value);
        break;
      }

      case delete_query: {
        current_db->del(e.key);
        break;
      }

      case update_query: {
        current_db->update(e.key, e.value);
        break;
      }

      default: { throw runtime_error("Unsupported workload command"); }
    }
  }

  cout << endl;

  return res;
}
