## TODO
1. Vector:
   1. hasNull
   2. get -> ScalarKind
   3.  get_data, get_data_mut 拿到底层数据的借用/可变借用
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

## Type conversion
类型转换
1. 引用
   1. VectorKind -> 具体的 Vector 类型
2. 所有权：实现 From 和 TryFrom 特征
   1. VectorKind -> 具体的 Vector 类型

## Future
1. any vector
2. Table:
   1. get_table_type
   2. num_column
   3. num_row
   4. size (num_column * num_row)
   5. get_column_name, get_column_name_mut
   6. get_column, get_column_mut
   7. get_row 返回字典
3. run 返回 ConstantKind，size > 1 时返回 VectorKind

