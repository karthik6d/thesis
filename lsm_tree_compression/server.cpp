#include "server.h"
#include "lsm_tree.h"

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

using namespace std;

vector<workload_entry> workload;

void create(string db_name, int is_compressed) {
  LSM_Tree* db = new LSM_Tree();
  db->name = db_name;
  db->compressed = is_compressed;
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

  int i = 0;

  int lines = count(data_str.begin(), data_str.end(), '\n');

  cout << "reading csv";

  istringstream ss(data_str);

  string line;

  while (getline(ss, line)) {
    // cout << "\r" << ++i << "/" << lines << flush;

    istringstream ss(line);

    string key_s, value_s;

    getline(ss, key_s, ',');
    getline(ss, value_s, ',');

    current_db->write(stoi(key_s), stoi(value_s));
  }

  cout << endl;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Need to pass in query file or compressed state" << endl;
    return 1;
  }

  string output_str;
  int is_compressed = atoi(argv[1]);
  ifstream query_file(argv[2]);

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
      create(elements.at(1), is_compressed);
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

  cout << "starting workload";
  clock_t start;
  double duration;
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
