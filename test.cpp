#include <iostream>
#include <vector>
#include "snappy.h"

using namespace std;

int main()
{
    vector<int> a;
    for(int i = 1; i < 11; i++){
        a.push_back(1);
    }

    char* buf = (char*)a.data();
    string compressed;

    snappy::Compress((char*)buf, a.size() * sizeof(int), &compressed);
    
    cout << "input size:" << a.size() * sizeof(int) << " output size: " << compressed.size() << endl;

    string uncompressed;
    snappy::Uncompress(compressed.data(), compressed.size(), &uncompressed);

    const char* new_buffer = uncompressed.c_str();
    int a_new[10];
    memcpy(&a_new, new_buffer, 40);

    vector<int> new_a = vector<int>(a_new, a_new + 10);

    for(int i = 0; i < new_a.size(); i++){
        cout << new_a[i] << "\t";
    }

    return 1;
}