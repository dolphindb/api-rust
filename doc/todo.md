## TODO
1. Null 在 DDB 是类型最小值，这个不一致导致的序列化问题，不能用 Option(rawtype) 当标量类型的内部结构，不然 () 和 rawtype::min_value 都是 null 有问题
<!-- $rawtype = (bool/char/short/int/long/float/double/string/date/month/time/minute/second/date_time/time_stamp/nano_time/nano_time_stamp) -->
1. Basic
   1. [x] data_type
   2. [x] data_form
   3. [x] data_category
   4. [x] size
   5. [x] is_empty
2. Constant:
   1. [x] isScalar
   2. [x] isVector
   3. [x] isPair
   4. isTable
   5. [x] isSet
   6. [x] isDictionary
3. Scalar:
   1. [x] is_null
   2. [/] get_$rawtype (具体的标量类型也实现同名的 get_\$rawtype，不过参数返回不同)
   3. [ ] set_$rawtype (具体的标量类型也实现同名的 set_\$rawtype，不过参数返回不同)
4. Vector:
   1. clear
   2. hasNull
   3. get -> ScalarKind
   4. set(ScalarKind)
   5. append
   6. append_$rawtype
   7. get_$rawtype
   8. set_$rawtype
   9.  get_data, get_data_mut 拿到底层数据的借用/可变借用
5. Set:
   1. clear
   2. contain
   3. append
   4. insert
6. Dictionary:
   1. set
   2. get_member
   3. get_key_type
   4. keys, keys_mut 拿到底层数据的借用/可变借用
   5. values, values_mut 拿到底层数据的借用/可变借用
7. Table:
   1. get_table_type
   2. num_column
   3. num_row
   4. size (num_column * num_row)
   5. get_column_name, get_column_name_mut
   6. get_column, get_column_mut
   7. get_row 返回字典
8.  to_string -> 全部实现 Display
9.  run 返回 ConstantKind，size > 1 时返回 VectorKind
10. new, from_data_type 每个类型?? 修改

## Type conversion
类型转换
1. 引用
   1. Constant
      1. ConstantKind -> ScalarKind
      2. ConstantKind -> VectorKind
      3. ConstantKind -> SetKind
      4. ConstantKind -> DictionaryKind
      5. ConstantKind -> TableKind
   2. Scalar
      1. ScalarKind -> Bool
      2. ScalarKind -> Char
      3. ScalarKind -> Short
      4. ScalarKind -> Int
      5. ScalarKind -> Float
      6. ScalarKind -> String
      7. ..
2. 所有权：实现 From 和 TryFrom 特征，支持的转换类型和引用类似(除了不是引用)
3. string 序列化 null 是什么？？

## Future
1. any vector

| DolphinDB 类型 | Rust 类型         | 类型值 |
| -------------- | ----------------- | ------ |
| Void           | -                 | 0      |
| Bool           | u8                | 1      |
| Char           | u8                | 2      |
| Short          | i16               | 3      |
| Int            | i32               | 4      |
| Long           | i64               | 5      |
| Date           | i32               | 6      |
| Month          | i32               | 7      |
| Time           | i32               | 8      |
| Minute         | i32               | 9      |
| Second         | i32               | 10     |
| DateTime       | i32               | 11     |
| TimeStamp      | i64               | 12     |
| NanoTime       | i64               | 13     |
| NanoTimeStamp  | i64               | 14     |
| Float          | OrderedFloat<f32> | 15     |
| Double         | OrderedFloat<f64> | 16     |
| DolphinString  | String            | 18     |
| DateHour       | i32               | 28     |
