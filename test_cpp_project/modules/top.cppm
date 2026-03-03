// -*- c++ -*-
module;

#include <concepts>
#include <type_traits>
#include <limits>

export module top;

export void print_something(char const *a = "", char const *b = "");
export void print_value(unsigned long value);
export void print_value(long value);
export void print_value(double value);

export template <typename T>
concept numeric = ::std::integral<T> || ::std::floating_point<T>;

export template <numeric Base>
Base constexpr intpow(Base base, ::std::integral auto exp)
{
   using exp_t = decltype(exp);
   auto tail_recurse = [](this auto &&self, Base to_pow2, Base accum, exp_t exp) {
      auto const squared = to_pow2 * to_pow2;
      auto const next_exp = exp / 2;
      if (exp == 0) {
         return accum;
      } else if (exp == 1) {
         return accum * to_pow2;
      } else if (exp & 1) {
         return self(squared, accum * to_pow2, next_exp);
      } else {
         return self(squared, accum, next_exp);
      }
   };
   if (exp < 0) {
      if constexpr (::std::is_integral_v<Base>) {
         return 0;
      }
      constexpr Base one = 1;
      if (exp <= ::std::numeric_limits<exp_t>::min()) {
	 // The most negative integer of any type is a power of 2.
	 auto tmp = intpow(base, -(exp / 2));
	 return one / (tmp * tmp);
      } else {
         return one / intpow(base, -exp);
      }
   }
   return tail_recurse(base, 1, exp);
}

auto fptr_1 = intpow<unsigned int, unsigned int>;
auto fptr_2 = intpow<int, unsigned int>;
auto fptr_3 = intpow<unsigned int, int>;
auto fptr_4 = intpow<int, int>;
auto fptr_5 = intpow<unsigned long, unsigned long>;
auto fptr_6 = intpow<long, unsigned long>;
auto fptr_7 = intpow<unsigned long, long>;
auto fptr_8 = intpow<long, long>;
auto fptr_9 = intpow<float, int>;
auto fptr_10 = intpow<float, unsigned int>;
auto fptr_11 = intpow<float, long>;
auto fptr_12 = intpow<float, unsigned long>;
auto fptr_13 = intpow<double, int>;
auto fptr_14 = intpow<double, unsigned int>;
auto fptr_15 = intpow<double, long>;
auto fptr_16 = intpow<double, unsigned long>;
