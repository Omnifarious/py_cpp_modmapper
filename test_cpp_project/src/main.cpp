import top;
import middle_a;
import middle_b;
import bottom;

int main()
{
   print_something("thing1", "thing2");
   ma_ps("ma_ps thing");
   mb_ps("mb_ps thing");
   print_something_twice();
   constexpr auto foo = intpow(double{1.5}, 20U);
   print_value(foo);
   constexpr auto bar = intpow(3UL, 14U);
   print_value(bar);
   return 0;
}
