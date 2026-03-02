module;

#include <iostream>

module top;

void print_something(char const *a, char const *b)
{
   ::std::cout << "Top: a == \"" << a << "\" && b == \"" << b << "\"\n";
}
