1. Null 在 DDB 是类型最小值，这个不一致导致的序列化问题，不能用 Option(raw_type) 当标量类型的内部结构，不然 () 和 raw_type::min_value 都是 null 有问题
2. Constant
   1. data_type
   2. data_form
   3. data_category
   4. get
   5. len
   6. is_empty
   7. is_null
3. Scalar: Constant
   1. 
4. Vector: Constant
   1. 
5. Set: Constant
   1. 
6. Dictionary: Constant
   1. 
7. Table: Constant
   1. getTableType
8. to_string -> 全部实现 Display
9. decimal 类型
10. any vector
11. bool char 类型的范围错了，参考文档和序列化