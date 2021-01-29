#ifndef SERVER_H
#define SERVER_H

#include "lsm_tree.h"

using namespace std;

enum query_type { read_query, write_query, update_query, delete_query };

typedef struct workload_entry {
  query_type type;
  int key;
  int value;
} workload_entry;

// Declarations for server.cpp
void create(string db_name);
void load(string path);
int binary_search(vector<kv> data, int x);
component create_component(vector<kv> kvs);
vector<int> execute_workload();

#endif
