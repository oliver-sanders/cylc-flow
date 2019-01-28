CyclePoint
==========

Integer
-------

* add
* cmp_
* sub
  * point
  * interval
* standardise
* __int__
* __hash__

ISO8601
-------

* add
* __cmp__
* standardise
* sub
  * point
  * interval
* __hash__

isodatetime
-----------

* __eq__, etc
* __add__
  * point
  * duration
* __sub__
  * point
  * duration

Functionality Gap
-----------------

* standardise


Interval
========

Integer
-------

* get_null
* get_null_offset ? used once
* add
* cmp_
* sub
* __abs__
* __mul__
* __bool__

isodatetime
-----------

* get_null => __init__()
* __add__
  * point
  * duration
* __sub__
  * point
  * duration
* __eq__, etc
* __abs__
* __mul__
* __bool__


Functionality Gap
-----------------

get_null_offset


Sequence
========

ISO8601
-------

* get_async_expr  -  get first point in native syntax
* get_interval
* is_on_sequence
* is_valid  -  same as is_on_sequence with additional bounds check
* get_nearest_prev_point
* get_next_point  -  next from point
* get_next_point_on_sequence  -  next after arbitrary point
* get_first_point
* get_start_point
* get_stop_point
* __eq__

isodatetime
-----------

* TimeRecurrence.duration
* get_is_valid
* get_prev
* get_next
* TimeRecurrence.start_point, min_point
* TimeRecurrence.end_point, max_point

Functionalirt Gap
-----------------

* get_async_expr
* is_on_sequence - see get_is_valid
* get_next_point_on_sequence - see get_next
* __lt__, etc


init
====

* num_expanded_year_digits
* custom_dump_format
* time_zone
* assume_utc
* cycling_mode (calendar)
