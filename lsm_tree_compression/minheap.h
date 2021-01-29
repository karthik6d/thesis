#ifndef MIN_HEAP_H
#define MIN_HEAP_H

#include <assert.h>
#include <iostream>
#include <vector>

using namespace std;

// struct MinHeapNode {
//   // The element to be stored
//   int element;
//   // Value of the key
//   int value;
//   // index of the array from which the element is taken
//   int i;
// };

// // Function declarations
// void swap(MinHeapNode* x, MinHeapNode* y);

// A class for Min Heap
template <typename T>
struct minheap {
  vector<T> elements;  // elements of the heap
  unsigned int num_elements;
  int (*compare)(T x, T y);

  // constructor of a heap of the size
  minheap(int (*compare)(T, T)) {
    this->num_elements = 0;
    this->compare = compare;
  }

  // to get index of left child of node at index i
  int left(int i) { return 2 * i + 1; }

  // to get index of right child of node at index i
  int right(int i) { return 2 * i + 2; }

  // get the parent of a node
  int parent(int i) { return (i - 1) / 2; }

  T peek() { return elements[0]; }

  T pop() {
    assert(this->num_elements > 0);
    T res = this->elements[0];

    this->elements[0] = this->elements[this->num_elements - 1];
    --this->num_elements;

    this->minheapify(0);

    return res;
  }

  void minheapify(unsigned int i) {
    unsigned int l = this->left(i);
    unsigned int r = this->right(i);
    unsigned int smallest = i;

    if (l < this->num_elements &&
        this->compare(this->elements[l], this->elements[smallest]) < 0) {
      smallest = l;
    }

    if (r < this->num_elements &&
        this->compare(this->elements[r], this->elements[smallest]) < 0) {
      smallest = r;
    }

    if (smallest != i) {
      this->swap(i, smallest);

      this->minheapify(smallest);
    }
  }

  void swap(int i, int j) {
    T tmp = this->elements[i];
    this->elements[i] = this->elements[j];
    this->elements[j] = tmp;
  }

  void insert(T elem) {
    if (this->num_elements == this->elements.size()) {
      this->elements.push_back(elem);
    } else {
      this->elements[this->num_elements] = elem;
    }

    int i = this->num_elements++;

    while (i != 0 &&
           compare(this->elements[i], this->elements[this->parent(i)]) < 0) {
      swap(i, parent(i));
      i = parent(i);
    }
  }

  int size() { return this->num_elements; }
};

// // Constructor: Builds a heap from a given array a[]
// // of given size
// MinHeap::MinHeap(MinHeapNode a[], int size) {
//   heap_size = size;
//   harr = a;  // store address of array
//   int i = (heap_size - 1) / 2;
//   while (i >= 0) {
//     MinHeapify(i);
//     i--;
//   }
// }

// // A recursive method to heapify a subtree with root
// // at given index. This method assumes that the
// // subtrees are already heapified
// void MinHeap::MinHeapify(int i) {
//   int l = left(i);
//   int r = right(i);
//   int smallest = i;
//   if (l < heap_size && harr[l].element < harr[i].element) smallest = l;
//   if (r < heap_size && harr[r].element < harr[smallest].element) smallest =
//   r; if (smallest != i) {
//     swap(&harr[i], &harr[smallest]);
//     MinHeapify(smallest);
//   }
// }

// // A utility function to swap two elements
// void swap(MinHeapNode* x, MinHeapNode* y) {
//   MinHeapNode temp = *x;
//   *x = *y;
//   *y = temp;
// }

#endif
