#ifndef MERGE_H
#define MERGE_H

#include "lsm_tree.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

using namespace std;

typedef struct kvi {
  kv k;
  unsigned int index;
} kvi;

int compare_kvi(kvi x, kvi y);

component merge(vector<component> components);

#endif
