## TODO
1. Vector:
   1. clear
   2. hasNull
   3. get -> ScalarKind
   4. set(ScalarKind)
   5. append
   6. append_$rawtype
   7. get_$rawtype
   8. set_$rawtype
   9.  get_data, get_data_mut 拿到底层数据的借用/可变借用
2. Set:
   1. clear
   2. contain
   3. append
   4. insert
3. Dictionary:
   1. set
   2. get_member
   3. get_key_type
   4. keys, keys_mut 拿到底层数据的借用/可变借用
   5. values, values_mut 拿到底层数据的借用/可变借用
4. Table:
   1. get_table_type
   2. num_column
   3. num_row
   4. size (num_column * num_row)
   5. get_column_name, get_column_name_mut
   6. get_column, get_column_mut
   7. get_row 返回字典
5.  to_string -> 全部实现 Display
6.  run 返回 ConstantKind，size > 1 时返回 VectorKind
7.  new, from_data_type 每个类型?? 修改

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
