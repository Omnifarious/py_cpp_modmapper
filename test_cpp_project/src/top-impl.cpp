module;

#include <iostream>

module top;

void print_something(char const *a, char const *b)
{
   ::std::cout << "Top: a == \"" << a << "\" && b == \"" << b << "\"\n";
}

void print_value(unsigned long value)
{
   ::std::cout << "Value: " << value << "\n";
}

void print_value(long value)
{
   ::std::cout << "Value: " << value << "\n";
}

void print_value(double value)
{
   ::std::cout << "Value: " << value << "\n";
}
