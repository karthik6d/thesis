#include "compression.h"
#include "merge.h"

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
#include <stdlib.h>

using namespace std;

LSM_Tree* current_db = nullptr;
int component_count = 0;

void LSM_Tree::write(int key, int value) {
  // if it fits into the top-level buffer, just add it there
  if (this->buffer.size() < DEFAULT_BUFFER_SIZE) {
    this->buffer.push_back({key, value});
    return;
  }

  // otherwise, we have to create a new component for it
  component c(this->buffer);
  this->insert_component(c);
  this->buffer.clear();
  this->buffer.push_back({key, value});
}

void LSM_Tree::del(int key) { this->write(-key, 0); }

void LSM_Tree::update(int key, int value) { this->write(key, value); }

// comparison function between key value pairs
int compare_kvs(kv a, kv b) {
  int a_val = a.key < 0 ? -a.key : a.key;
  int b_val = b.key < 0 ? -b.key : b.key;
  return a_val < b_val;
}

subcomponent::subcomponent(vector<kv> kvs) {
  this->min_value = INT32_MAX;
  this->max_value = INT32_MIN;

  // Create the bloom filter
  bloom_parameters parameters;
  // How many elements roughly do we expect to insert?
  parameters.projected_element_count = DEFAULT_BUFFER_SIZE;
  // Maximum tolerable false positive probability? (0,1)
  parameters.false_positive_probability = 0.01; // 1 in 100
  // Compute the optimal parameters
  parameters.compute_optimal_parameters();
  bloom_filter filter(parameters);
  
  this->filter = filter;

  this->num_values = kvs.size();

  // Will need to do some data sketching here before any compression scheme is sent
  for (auto pair : kvs) {
    int val = pair.key < 0 ? -pair.key : pair.key;
    this->filter.insert(pair.key);

    if (val < min_value) {
      this->min_value = val;
    }

    if (val > max_value) {
      this->max_value = val;
    }
  }

  cout << "Has to be here" << endl;
  // Print out what the R's should be to hold this leniency model
  vector<string> compression_schemes;
  compression_schemes.push_back("snappy");
  compression_schemes.push_back("simd");
  compression_schemes.push_back("rle");
  compression_schemes.push_back("zlib");
  compression_schemes.push_back("zstandard");

  unordered_map<string, float> optimal_rs;

  for(int i = 0; i < compression_schemes.size(); i++) {
    optimal_rs[compression_schemes.at(i)] = optimal_r(current_db -> constants, current_db->read_only, current_db->write_only, current_db->leniency, compression_schemes.at(i));
  }

  cout << "womp womp" << endl;

  vector<float> hist = histogram(kvs, 50);

  cout << "is something wrong with creating the histogram";
  // Do the prediction here
  int best_compression = 0;
  float best_compression_ratio = 1.0;

  // cout << "Does something break" << endl;
  // for(int i = 0; i < compression_schemes.size(); i++) {
  //   float predicted_r;
  //   cout << "Here" << endl;
  //   current_db->models[compression_schemes.at(i)].rf->predict(hist.data(), predicted_r);
  //   cout << "Prediction breaks" << endl;
  //   float max_optimal_r = optimal_rs[compression_schemes.at(i)];
  //   float diff = abs(predicted_r - max_optimal_r);
  //   cout << compression_schemes.at(i) << ": Predicted R: " << predicted_r << " Max_Optimal_R: " << max_optimal_r << endl;
  //   if(diff < 0.05 && predicted_r < best_compression_ratio) {
  //     best_compression = i+1;
  //     best_compression_ratio = predicted_r;
  //   }
  // }
  cout << "Best Compression: " << best_compression << endl;
  cout << "Optimal Compression Ratio: " << best_compression_ratio << endl;
  this->compressed = best_compression;
  // Have to add the compressed as well
  if(this->compressed == 1){
    this->filename = SNAPPY_encode(kvs);
  }
  else if(this->compressed == 2){
    this->filename = SIMD_encode(kvs);
  }
  else if(this->compressed == 3){
    this->filename = RLE_encode(kvs);
  }
  else if(this->compressed == 4){
    this->filename = ZLIB_encode(kvs);
  }
  else if(this->compressed == 5){
    this->filename = ZSTANDARD_encode(kvs);
  }
  else {
    this->filename = string("data/C");
    this->filename.append(to_string(component_count++));
    this->filename.append(".dat");

    ofstream data_file(this->filename, ios::binary);

    //writing the key value pairs to the data file
    data_file.write((char*)kvs.data(), kvs.size() * sizeof(kv));
    data_file.close();
  }
}

component::component(vector<kv> kvs) {
  unordered_map<int, int> m;

  // iterate over the vector and collect the last updates
  for (kv k : kvs) {
    m.erase(-k.key);
    m[k.key] = k.value;
  }

  // create the final list of updates
  vector<kv> final_kvs;

  for (auto p : m) {
    final_kvs.push_back({p.first, p.second});
  }

  // sort the list of updates
  std::sort(final_kvs.begin(), final_kvs.end(), compare_kvs);

  // after the sorting and collecting, there should never be a negative key
  for (kv k : final_kvs) {
    assert(k.key >= 0);
  }

  for (unsigned int i = 0; i < final_kvs.size();) {
    int start = i;
    int end = min((size_t)i + DEFAULT_BUFFER_SIZE, final_kvs.size());

    this->subcomponents.push_back(subcomponent(
        vector<kv>(final_kvs.begin() + start, final_kvs.begin() + end)));

    i = end;
  }
}

void LSM_Tree::insert_component(component c) {
  unsigned int i = 0;

  pair<bool, component> res;

  do {
    if (i >= this->levels.size()) {
      this->levels.push_back(level());
    }

    res = this->levels[i++].insert_component(c);

    c = res.second;
  } while (res.first);
}

pair<read_result, int> LSM_Tree::read(int key) {
  // look through buffer first (go in reverse)
  for (auto it = this->buffer.rbegin(); it != this->buffer.rend(); ++it) {
    auto k = *it;

    if (k.key == key) {
      return pair<read_result, int>(found, k.value);
    } else if (k.key == -key) {
      return pair<read_result, int>(found, 0);
    }
  }

  // go through each level and try to read
  for (level l : this->levels) {
    pair<read_result, int> res = l.read(key);

    //cout << "Trying to figure out ordering" << endl;
    if (res.first == found || res.first == deleted) {
      return res;
    }
  }

  return pair<read_result, int>(not_found, 0);
}

pair<bool, component> level::insert_component(component c) {
  if (this->components.size() < COMPONENTS_PER_LEVEL) {
    this->components.push_back(c);

    return pair<bool, component>(false, component());
  }

  auto new_c = merge(this->components);

  this->components.clear();
  this->components.push_back(c);

  return pair<bool, component>(true, new_c);
}

pair<read_result, int> level::read(int key) {
  for (auto it = this->components.rbegin(); it != this->components.rend();
       ++it) {
    auto c = *it;

    pair<read_result, int> res = c.read(key);

    //cout << "Level::Read again trying to figure out ordering" << endl;
    if (res.first == found || res.first == deleted) {
      return res;
    }
  }

  return pair<read_result, int>(not_found, 0);
}

vector<kv> component::get_kvs() {
  vector<kv> res;
  for (auto sub : this->subcomponents) {
    vector<kv> s = sub.get_kvs();
    res.insert(res.end(), s.begin(), s.end());
  }

  return res;
}

pair<read_result, int> component::read(int key) {
  for (auto sub : this->subcomponents) {
    if (key < sub.min_value || key > sub.max_value || !sub.filter.contains(key)) {
      continue;
    }

    pair<read_result, int> res = sub.read(key);

    if (res.first == found || res.first == deleted) {
      return res;
    }
  }

  return pair<read_result, int>(not_found, 0);
}

component_iterator component::begin() {
  assert(this->subcomponents.size() > 0);
  //cout << "OK i guess here" << endl;
  return {.pos = 0,
          .it = this->subcomponents[0].begin(),
          .end = this->subcomponents[0].end(),
          .subcomponents = this->subcomponents};
}

component_iterator component::end() {
  assert(this->subcomponents.size() > 0);
  return {.pos = this->subcomponents.size(),
          .it = this->subcomponents[this->subcomponents.size() - 1].end(),
          .end = this->subcomponents[this->subcomponents.size() - 1].end(),
          .subcomponents = this->subcomponents};
}

vector<kv> subcomponent::get_kvs() {
  if(this->compressed == 1){
    kv* buf = SNAPPY_decode(this->filename);
    
    return vector<kv>(buf, buf + this->num_values);
  }

  else if(this->compressed == 2){
    kv* buf = SIMD_decode(this->filename);
    
    return vector<kv>(buf, buf + this->num_values);
  }

  else if(this->compressed == 3){
    kv* buf = RLE_decode(this->filename);

    return vector<kv>(buf, buf + this->num_values);
  }
  
  else if(this->compressed == 4){
    kv* buf = ZLIB_decode(this->filename);

    return vector<kv>(buf, buf + this->num_values);
  }

  else if(this->compressed == 5) {
    kv* buf = ZSTANDARD_decode(this->filename);

    return vector<kv>(buf, buf + this->num_values);
  }

  else {
    ifstream f(this->filename, ios::ate | ios::binary);

    // get the length of the file
    int length = f.tellg();

    // the file must be length 8
    assert(length % sizeof(kv) == 0);

    // go back to the beginning
    f.seekg(0, f.beg);

    // prepare the array and read in the data
    kv buf[length / sizeof(kv)];
    f.read((char*)buf, length);

    f.close();

    return vector<kv>(buf, buf + length / sizeof(kv));
  }
}

pair<read_result, int> subcomponent::read(int key) {
  for (kv k : this->get_kvs()) {
    if (k.key == key) {
      return pair<read_result, int>(found, k.value);
    }

    if (k.key == -key) {
      return pair<read_result, int>(deleted, 0);
    }
  }

  return pair<read_result, int>(not_found, 0);
}

subcomponent_iterator subcomponent::begin() {
  return {.pos = 0, .kvs = this->get_kvs()};
}

subcomponent_iterator subcomponent::end() {
  //vector<kv> tmp = this->get_kvs();
  return {.pos = this->num_values, .kvs = vector<kv>()};
}
