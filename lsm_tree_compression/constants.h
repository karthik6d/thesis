#pragma once
#include <unordered_map>
#include "RandomForests/RandomForest.h"

typedef struct dataset{
    float** features;
    float* values;
    RandomForest* rf;
} dataset;

std::unordered_map<std::string, float> write_constants_to_file();

dataset get_training_data(std::string path, int TRAIN_NUM, int FEATURE);