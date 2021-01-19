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

  // Have to add the compressed as well
  // creating the data file
  // this->filename = string("data/C");
  // this->filename += to_string(component_count++);
  if (current_db->compressed == 1) {
    ////cout << "Encoding with RLE" << endl;
    ofstream data_file("data/tmp", ios::binary);

    // writing the key value pairs to the data file
    data_file.write((char*)kvs.data(), kvs.size() * sizeof(kv));
    data_file.close();

    char* output_filename;

    Status status = rle_delta_file_encode("data/tmp", &output_filename);

    (void)status;

    this->filename = string(output_filename);

  } 
  else if(current_db->compressed == 2){
    ////cout << "Encoding with snappy" << endl;
    this->filename = snappy_array_encode((char*)kvs.data(), kvs.size()*sizeof(kv));
  }
  else if(current_db->compressed == 3){
    // //cout << "data" << endl;
    // for(kv val : kvs){
    //   //cout << val.key << ":" << val.value << "\t";
    // }
    //cout << endl;
    this->filename = SIMD_encode(kvs);
  }
  else {
    ////cout << "Normal uncompressed encoding" << endl;
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

    //cout << "When does decode even happen??" << endl;

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
  // Remove first four lines of code
  if (current_db->compressed == 1) {
    //cout << "before rle decode " << endl;
    size_t number_read = 0;

    kv* buf = (kv*) rle_delta_stream_decode(this->filename.c_str(), this->num_values * 2, &number_read);
    //cout << "after rle decode" << endl;
    //cout << this->num_values << endl;
    //cout << this->filename << endl;
    //cout << number_read << endl;
    // for(int i = 0; i < this->num_values; i++)
    // {
    //   //cout << buf[i].key << ":" << buf[i].value << "\t";
    // }

    return vector<kv>(buf, buf + this->num_values);
  }
  else if(current_db->compressed == 2){
    ifstream f(this->filename, ios::ate | ios::binary);

    const char* decoded_data = snappy_array_decode(this->filename);
    kv buf[this->num_values];
    memcpy(buf, decoded_data, this->num_values*sizeof(kv));
    
    return vector<kv>(buf, buf + this->num_values);
  }
  else if(current_db ->compressed == 3){
    ifstream f(this->filename, ios::ate | ios::binary);

    kv* buf = SIMD_decode(this->filename);
    
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
  //cout << "Reading a key in subcomponent read: " << endl;
  for (kv k : *this) {
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
  //cout << "Subcomponent Iterator BEGIN back tracking" << endl;
  return {.pos = 0, .kvs = this->get_kvs()};
}

subcomponent_iterator subcomponent::end() {
  //cout << "Ok this is annoying" << endl;
  vector<kv> tmp = this->get_kvs();
  //cout << "Subcomponent Iterator END back tracking" << endl;
  return {.pos = tmp.size(), .kvs = vector<kv>()};
}
