#include "SIMDCompressionAndIntersection/include/codecfactory.h"
#include "SIMDCompressionAndIntersection/include/intersection.h"

using namespace SIMDCompressionLib;

void SIMD(){
    IntegerCODEC &codec = *CODECFactory::getFromName("s4-fastpfor-d1");
}