1. Null 在 DDB 是类型最小值，这个不一致导致的序列化问题，不能用 Option(raw_type) 当标量类型的内部结构，不然 () 和 raw_type::min_value 都是 null 有问题
<!-- $raw_type = bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time,nano_time_stamp -->
1. Basic
   1. [x] data_type
   2. [x] data_form
   3. [x] data_category
   4. [x] size
   5. [x] is_empty
2. Constant:
   1. isScalar
   2. isVector
   3. isTuple
   4. isPair
   5. isTable
   6. isSet
   7. isDictionary
3. Scalar: Constant
   1. is_null
   2. get
   3. set
   4. get_$raw_type
   5. set_$raw_type
4. Vector: Constant
   1. clear
   2. hasNull
   3. get
   4. set
   5. append
   6. append_$raw_type
   7. get_$raw_type_by_index
   8. set_$raw_type_by_index
   9.  get_data, get_data_mut 拿到底层数据的借用/可变借用
5. Set: Constant
   1. clear
   2. contain
   3. append?
6. Dictionary: Constant
   1. set
   2. get_member
   3. get_key_type
   4. keys, keys_mut 拿到底层数据的借用/可变借用
   5. values, values_mut 拿到底层数据的借用/可变借用
7. Table: Constant
   1. get_table_type
   2. num_column
   3. num_row
   4. size (num_column * num_row)
   5. get_column_name, get_column_name_mut
   6. get_column, get_column_mut
   7. get_row 返回字典
8.  to_string -> 全部实现 Display
9.  decimal 类型
10. any vector
11. 类型转换，更容易地从 ConstantKind 转换为 下层的类型
12. bool char 类型的范围错了，参考文档和序列化
13. 删掉 len，因为有了 size
14. run 返回 Vec<ConstantKind>,什么时候size不为1
15. new, from_data_type 每个类型??
