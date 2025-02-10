#[macro_export]
macro_rules! vector_build {
    ($type:ty, $($x:expr),*) => {{
        let mut result=Vector::<$type>::with_capacity(3);
        $(
            result.push($x);
        )*
        result
    }};
}

#[macro_export]
macro_rules! set_build {
    ($type:ty, $($x:expr),*) => {{
        let mut result=Set::<$type>::with_capacity(3);
        $(
            result.insert($x);
        )*
        result
    }};
}

#[macro_export]
macro_rules! dictionary_build {
    ($type:ty, $($key:expr => $value:expr),*) => {{
        let mut result=Dictionary::<$type>::with_capacity(3);
        $(
            result.insert($key, $value);
        )*
        result
    }};
}

#[macro_export]
macro_rules! dictionary_build_any {
    ($type:ty, $($key:expr => $value:expr),*) => {{
        let mut result=Dictionary::<$type>::new();
        $(
            result.insert_any($key, $value);
        )*
        result
    }};
}

#[macro_export]
macro_rules! table_build {
    ($($column_name:expr => $columns:expr),*) => {{
        // case only need size 1
        let mut _column_name=Vec::<String>::with_capacity(3);
        let mut _columns=Vec::<VectorImpl>::with_capacity(3);
        $(
            _column_name.push($column_name);
            _columns.push($columns.into());
        )*
        let mut result=TableBuilder::new();
        result.with_contents(_columns,_column_name);
        result.build().unwrap()
    }};
}

#[macro_export]
macro_rules! array_vector_build {
    ($type:ty, $($value:expr),*) => {{
        let mut result = ArrayVector::<$type>::new();
        $(
            result.push($value);
        )*
        result
    }};
}
