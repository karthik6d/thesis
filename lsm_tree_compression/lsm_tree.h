#ifndef LSM_TREE_H
#define LSM_TREE_H

#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>
#include "bloom_filter.hpp"

// Defines number of key values pairs in Main Memory
#define DEFAULT_BUFFER_SIZE 1024

// Defines the number of components per level before merging
#define COMPONENTS_PER_LEVEL 4

using namespace std;

enum read_result { found, not_found, deleted };

typedef struct kv {
  int key;
  int value;
} kv;

typedef struct subcomponent_iterator {
  long unsigned int pos;
  vector<kv> kvs;

  subcomponent_iterator& operator++() {
    this->pos++;
    return *this;
  };

  subcomponent_iterator operator++(int) {
    subcomponent_iterator tmp = *this;
    ++*this;

    return tmp;
  };

  kv operator*() { return kvs[this->pos]; };

  bool operator==(subcomponent_iterator& rhs) { return pos == rhs.pos; };

  bool operator!=(subcomponent_iterator& rhs) { return pos != rhs.pos; };
} subcomponent_iterator;

// Structure for subcomponent in LSM Tree
typedef struct subcomponent {
  string filename;
  int min_value;
  int max_value;
  bloom_filter filter;
  unsigned int num_values;

  subcomponent(vector<kv> kvs);

  pair<read_result, int> read(int key);
  vector<kv> get_kvs();

  subcomponent_iterator begin();
  subcomponent_iterator end();
} subcomponent;

typedef struct component_iterator {
  long unsigned int pos;
  subcomponent_iterator it;
  subcomponent_iterator end;
  vector<subcomponent> subcomponents;

  component_iterator& operator++() {
    this->it++;

    if (this->it == this->end) {
      this->pos++;

      if (this->pos < this->subcomponents.size()) {
        this->it = this->subcomponents[this->pos].begin();
        this->end = this->subcomponents[this->pos].end();
      }
    }

    return *this;
  };

  component_iterator operator++(int) {
    component_iterator tmp = *this;
    ++*this;

    return tmp;
  };

  kv operator*() { return *this->it; };

  bool operator==(component_iterator& rhs) { return this->pos == rhs.pos; };

  bool operator!=(component_iterator& rhs) { return !(*this == rhs); };
} component_iterator;

// Structure for a component in LSM Tree
typedef struct component {
  vector<subcomponent> subcomponents;

  component() : subcomponents(){};
  component(vector<kv> kvs);

  pair<read_result, int> read(int key);
  vector<kv> get_kvs();

  component_iterator begin();
  component_iterator end();
} component;

// Structure for a level in LSM Tree
typedef struct level {
  vector<component> components;
  int level_capacity;

  pair<read_result, int> read(int key);
  pair<bool, component> insert_component(component);
} level;

// Structure for LSM Tree
typedef struct LSM_Tree {
  string name;
  int compressed;
  vector<level> levels;
  vector<kv> buffer;

  void write(int key, int value);
  pair<read_result, int> read(int key);
  void del(int key);
  void update(int key, int value);
  void insert_component(component c);
} LSM_Tree;

extern LSM_Tree* current_db;
extern int component_count;

#endif
