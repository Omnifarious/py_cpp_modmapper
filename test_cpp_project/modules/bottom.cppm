// -*- c++ -*-

export module bottom;

import middle_a;
import middle_b;

export inline void print_something_twice()
{
   ma_ps("bottom");
   mb_ps("bottom");
}
