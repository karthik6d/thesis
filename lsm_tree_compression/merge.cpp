#include "merge.h"
#include "compression.h"
#include "minheap.h"

#include <sys/stat.h>
#include <climits>
#include <cstdio>
#include <iostream>
#include <vector>

using namespace std;

int compare_kvi(kvi x, kvi y) {
  if (x.k.key < y.k.key) {
    return -1;
  } else if (x.k.key > y.k.key) {
    return 1;
  }

  if (x.index > y.index) {
    return -1;
  } else if (x.index < y.index) {
    return 1;
  }

  return 0;
}

component merge(vector<component> components) {
  vector<component_iterator> component_iterators;
  vector<component_iterator> component_iterator_ends;

  component result;

  minheap<kvi> heap(compare_kvi);

  for (auto c : components) {
    component_iterators.push_back(c.begin());
    component_iterator_ends.push_back(c.end());
  }

  for (unsigned int i = 0; i < components.size(); ++i) {
    if (component_iterators[i] != component_iterator_ends[i]) {
      auto k = *component_iterators[i];
      heap.insert({k, i});
      component_iterators[i]++;
    }
  }

  vector<kv> kvs;

  while (heap.size() != 0) {
    // get the top element
    auto kvi = heap.pop();

    // insert that element into the results set
    kvs.push_back(kvi.k);

    // add back from the iterator that was removed from
    if (component_iterators[kvi.index] != component_iterator_ends[kvi.index]) {
      auto k = *component_iterators[kvi.index];
      heap.insert({k, kvi.index});
      component_iterators[kvi.index]++;
    }

    // while the heap still contains keys identical to what was just removed
    while (heap.size() != 0 && heap.peek().k.key == kvi.k.key) {
      // remove that element
      kvi = heap.pop();

      // add back from the iterator that was removed from
      if (component_iterators[kvi.index] !=
          component_iterator_ends[kvi.index]) {
        auto k = *component_iterators[kvi.index];
        heap.insert({k, kvi.index});
        component_iterators[kvi.index]++;
      }
    }

    if (kvs.size() == DEFAULT_BUFFER_SIZE) {
      result.subcomponents.push_back(subcomponent(kvs));
      kvs.clear();
    }
  }

  if (kvs.size() > 0) {
    result.subcomponents.push_back(subcomponent(kvs));
  }

  return result;
}
