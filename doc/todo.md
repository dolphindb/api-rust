1. Null 在 DDB 是类型最小值，这个不一致导致的序列化问题，不能用 Option(raw_type) 当标量类型的内部结构，不然 () 和 raw_type::min_value 都是 null 有问题
2. Basic
   1. [x] data_type
   2. [x] data_form
   3. data_category
   5. [x] size
   6. [x] is_empty
   7. [x] is_null
   8. is_number
   9. get_bool/char/short/int/long/float/double/string /date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp
   10. set_bool/char/short/int/long/float/double/string /date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp
   11. get_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_buffer
   12. set_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_buffer
   13. get_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_by_index
   15. set_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_by_index
   16. get_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_buffer_by_index
   17. set_bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp_buffer_by_index
   18. hasNull
   19. append
   10. remove
3. Constant:
   1. isScalar
   2. isVector
   3. isTuple
   4. isPair
   5. isTable
   6. isSet
   7. isDictionary
4. Scalar: Constant
5. Vector: Constant
   1. append_bool/char/short/int/long/float/double/string
   2. clear
   3. get
6. Set: Constant
   1. clear
   2. contain
7. Dictionary: Constant
   1. get_member
   2. get_key_type
   3. keys
   4. values
   5. set
8. Table: Constant
   1. getTableType
   2. columns
   3. rows
   4. size
   5. get_column_name
   6. get_column_names
   7. get_column
   8. get_columns
   9. get_row
9.  to_string -> 全部实现 Display
10. decimal 类型
11. any vector
12. 类型转换，更容易地从 ConstantKind 转换为 下层的类型
13. bool char 类型的范围错了，参考文档和序列化
14. 删掉 len，因为有了 size