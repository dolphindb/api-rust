extern crate libc;
use libc::{c_char, c_double, c_float, c_int, c_longlong, c_short, c_uchar, c_void};
use std::ffi::CStr;
use std::ffi::CString;
use std::str;
//extern {
//    fn double_input(input: libc::c_int) -> libc::c_int;
//}

pub const DT_VOID: c_int = 0;
pub const DT_BOOL: c_int = 1;
pub const DT_CHAR: c_int = 2;
pub const DT_SHORT: c_int = 3;
pub const DT_INT: c_int = 4;
pub const DT_LONG: c_int = 5;
pub const DT_DATE: c_int = 6;
pub const DT_MONTH: c_int = 7;
pub const DT_TIME: c_int = 8;
pub const DT_MINUTE: c_int = 9;
pub const DT_SECOND: c_int = 10;
pub const DT_DATETIME: c_int = 11;
pub const DT_TIMESTAMP: c_int = 12;
pub const DT_NANOTIME: c_int = 13;
pub const DT_NANOTIMESTAMP: c_int = 14;
pub const DT_FLOAT: c_int = 15;
pub const DT_DOUBLE: c_int = 16;
pub const DT_SYMBOL: c_int = 17;
pub const DT_STRING: c_int = 18;
pub const DT_UUID: c_int = 19;
pub const DT_FUNCTIONDEF: c_int = 20;
pub const DT_HANDLE: c_int = 21;
pub const DT_CODE: c_int = 22;
pub const DT_DATASOURCE: c_int = 23;
pub const DT_RESOURCE: c_int = 24;
pub const DT_ANY: c_int = 25;
pub const DT_COMPRESS: c_int = 26;
pub const DT_DICTIONARY: c_int = 27;

pub const DT_DATEHOUR: c_int = 28;
pub const DT_DATEMINUTE: c_int = 29;
pub const DT_IP: c_int = 30;
pub const DT_INT128: c_int = 31;
pub const DT_OBJECT: c_int = 32;

pub const DF_VECTOR: c_int = 0;
pub const DF_PAIR: c_int = 1;
pub const DF_MATRIX: c_int = 2;
pub const DF_SET: c_int = 3;
pub const DF_DICTIONARY: c_int = 4;
pub const DF_TABLE: c_int = 5;
pub const DF_CHART: c_int = 6;
pub const DF_CHUNK: c_int = 7;

fn stringc(s: &str) -> *mut c_char {
    CString::new(s).unwrap().into_raw()
}

fn stringr(c: *mut c_char) -> String {
    unsafe {
        return CStr::from_ptr(c).to_string_lossy().into_owned();
        // CString::new(s).unwrap().into_raw()
    }
}

pub trait Defobj {
    fn obj_ptr(&self) -> *mut c_void;

    fn size(&self) -> c_int {
        unsafe {
            return Constant_size(self.obj_ptr());
        }
    }

    fn to_constant(&self) -> Constant {
        unsafe {
            return Constant {
                c: toConstant(self.obj_ptr()),
            };
        }
    }

    fn get_string(&self) -> String {
        unsafe {
            //let r_str = CStr::from_ptr(Constant_getString(self.v));
            return stringr(Constant_getString(self.obj_ptr()));
        }
    }

    fn get_form(&self) -> c_int {
        unsafe {
            return Constant_getForm(self.obj_ptr());
        }
    }
    fn get_type(&self) -> c_int {
        unsafe {
            return Constant_getType(self.obj_ptr());
        }
    }

    // fn to_table(&self) -> Table{
    //    unsafe    {       return Table{t:toTable(self.obj_ptr())};  } }

    fn is_scalar(&self) -> bool {
        unsafe {
            return Constant_isScalar(self.obj_ptr());
        }
    }
    fn get(&self, index: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Constant_get(self.obj_ptr(), index),
            };
        }
    }

    fn set_binary_by_index(&self, index: c_int, buf: &[c_uchar]) {
        unsafe {
            Constant_setBinaryByIndex(self.obj_ptr(),index, buf.as_ptr());
        }
    }

    fn set_binary_slice(&self, start: c_int, len: c_int, buf: &[c_uchar]) -> bool {
        unsafe {
            return Constant_setBinaryArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn is_pair(&self) -> bool {
        unsafe {
            return Constant_isPair(self.obj_ptr());
        }
    }
    fn is_matrix(&self) -> bool {
        unsafe {
            return Constant_isMatrix(self.obj_ptr());
        }
    }
    fn is_vector(&self) -> bool {
        unsafe {
            return Constant_isVector(self.obj_ptr());
        }
    }
    fn is_table(&self) -> bool {
        unsafe {
            return Constant_isTable(self.obj_ptr());
        }
    }
    fn is_set(&self) -> bool {
        unsafe {
            return Constant_isSet(self.obj_ptr());
        }
    }
    fn is_dictionary(&self) -> bool {
        unsafe {
            return Constant_isDictionary(self.obj_ptr());
        }
    }

    fn is_large_constant(&self) -> bool {
        unsafe {
            return Constant_isLargeConstant(self.obj_ptr());
        }
    }

    fn set_bool_slice(&self, start: c_int, len: c_int, buf: &[bool]) -> bool {
        unsafe {
            return Constant_setBoolArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_int_slice(&self, start: c_int, len: c_int, buf: &[c_int]) -> bool {
        unsafe {
            return Constant_setIntArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_short_slice(&self, start: c_int, len: c_int, buf: &[c_short]) -> bool {
        unsafe {
            return Constant_setShortArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_long_slice(&self, start: c_int, len: c_int, buf: &[c_longlong]) -> bool {
        unsafe {
            return Constant_setLongArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_float_slice(&self, start: c_int, len: c_int, buf: &[c_float]) -> bool {
        unsafe {
            return Constant_setFloatArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_double_slice(&self, start: c_int, len: c_int, buf: &[c_double]) -> bool {
        unsafe {
            return Constant_setDoubleArray(self.obj_ptr(), start, len, buf.as_ptr());
        }
    }

    fn set_string_slice(&self, start: c_int, len: c_int, buf: &[&str]) -> bool {
        unsafe {
            for i in 0..(len as usize) {
                Constant_setStringArray(self.obj_ptr(), start + (i as c_int), 1, stringc(buf[i]));
            }
            return true;
        }
    }

    fn set_bool_by_index(&self, index: c_int, val: bool) {
        unsafe {
            Constant_setBoolByIndex(self.obj_ptr(), index, val);
        }
    }

    fn set_int_by_index(&self, index: c_int, val: c_int) {
        unsafe {
            Constant_setIntByIndex(self.obj_ptr(), index, val);
        }
    }

    fn set_long_by_index(&self, index: c_int, val: c_longlong) {
        unsafe {
            Constant_setLongByIndex(self.obj_ptr(), index, val);
        }
    }

    fn set_short_by_index(&self, index: c_int, val: c_short) {
        unsafe {
            Constant_setShortByIndex(self.obj_ptr(), index, val);
        }
    }
    fn is_null(&self) -> bool {
        unsafe {
            return Constant_isNull(self.obj_ptr());
        }
    }

    fn set_float_by_index(&self, index: c_int, val: c_float) {
        unsafe {
            Constant_setFloatByIndex(self.obj_ptr(), index, val);
        }
    }

    fn set_double_by_index(&self, index: c_int, val: c_double) {
        unsafe {
            Constant_setDoubleByIndex(self.obj_ptr(), index, val);
        }
    }

    fn set_string_by_index(&self, index: c_int, val: &str) {
        unsafe {
            Constant_setStringByIndex(self.obj_ptr(), index, stringc(val));
        }
    }

    fn set_null_by_index(&self, index: c_int) {
        unsafe {
            Constant_setNullByIndex(self.obj_ptr(), index);
        }
    }

    fn set_by_index(&self, index: c_int, val: Constant) -> bool {
        unsafe {
            return Constant_setByIndex(self.obj_ptr(), index, val.c);
        }
    }

    fn set_int(&self, val: c_int) {
        unsafe {
            Constant_setInt(self.obj_ptr(), val);
        }
    }
    fn set_bool(&self, val: bool) {
        unsafe {
            Constant_setBool(self.obj_ptr(), val);
        }
    }
    fn set_long(&self, val: c_longlong) {
        unsafe {
            Constant_setLong(self.obj_ptr(), val);
        }
    }
    fn set_short(&self, val: c_short) {
        unsafe {
            Constant_setShort(self.obj_ptr(), val);
        }
    }
    fn set_float(&self, val: c_float) {
        unsafe {
            Constant_setFloat(self.obj_ptr(), val);
        }
    }
    fn set_double(&self, val: c_double) {
        unsafe {
            Constant_setDouble(self.obj_ptr(), val);
        }
    }
    fn set_string(&self, val: &str) {
        unsafe {
            Constant_setString(self.obj_ptr(), stringc(val));
        }
    }
    fn set_null(&self) {
        unsafe {
            Constant_setNull(self.obj_ptr());
        }
    }
}

#[derive(Debug)]
pub struct Constant {
    c: *mut c_void,
}
impl Constant {
    pub fn get_int(&self) -> c_int {
        unsafe {
            return Constant_getInt(self.c);
        }
    }

    pub fn get_short(&self) -> c_short {
        unsafe {
            return Constant_getShort(self.c);
        }
    }
    pub fn get_long(&self) -> c_longlong {
        unsafe {
            return Constant_getLong(self.c);
        }
    }
    pub fn get_char(&self) -> c_char {
        unsafe {
            return Constant_getChar(self.c);
        }
    }
    pub fn get_float(&self) -> c_float {
        unsafe {
            return Constant_getFloat(self.c);
        }
    }
    pub fn get_double(&self) -> c_double {
        unsafe {
            return Constant_getDouble(self.c);
        }
    }

    pub fn get_index(&self) -> c_int {
        unsafe {
            return Constant_getIndex(self.c);
        }
    }

    pub fn get_bool(&self) -> bool {
        unsafe {
            return Constant_getBool(self.c);
        }
    }

    pub fn to_table(&self) -> Table {
        unsafe {
            return Table { t: toTable(self.c) };
        }
    }

    pub fn to_vector(&self) -> Vector {
        unsafe {
            return Vector {
                v: toVector(self.c),
            };
        }
    }
    pub fn to_matrix(&self) -> Matrix {
        unsafe {
            return Matrix {
                m: toMatrix(self.c),
            };
        }
    }

    pub fn to_set(&self) -> Set {
        unsafe {
            return Set { s: toSet(self.c) };
        }
    }
    pub fn to_dictionary(&self) -> Dictionary {
        unsafe {
            return Dictionary {
                d: toDictionary(self.c),
            };
        }
    }
}

impl Defobj for Constant {
    fn obj_ptr(&self) -> *mut c_void {
        self.c
    }
}

#[derive(Debug)]
pub struct DBConnection {
    conn: *mut c_void,
}

impl DBConnection {
    //   pub fn new() -> *mut c_void{
    pub fn new() -> DBConnection {
        unsafe {
            DBConnection {
                conn: DBConnection_new(),
            }
            //      self.conn =  DBConnection_new();
        }
    }

    pub fn connect(&self, host: &str, port: c_int, user: &str, pass: &str) -> bool {
        unsafe {
            return DBConnection_connect(
                self.conn,
                stringc(&host),
                port,
                stringc(&user),
                stringc(&pass),
            );
        }
    }
    pub fn run(&self, s: &str) -> Constant {
        unsafe {
            //   let  m : *mut c_void = DBConnection_run(self.conn, stringc(&s));
            //    let x:c_int = Constant_getInt(m);
            //     println!("{}",Constant_getInt(m));
            //   return Constant_getInt(m);
            return Constant {
                c: DBConnection_run(self.conn, stringc(&s)),
            };
        }
    }

    pub fn close(&self) {
        unsafe {
            DBConnection_close(self.conn);
        }
    }

    pub fn upload(&self, name: &str, c: Constant) {
        unsafe {
            DBConnection_upload(self.conn, stringc(&name), c.c);
        }
    }
    //buf:&[c_int]
    pub fn run_func(&self, script: &str, args: &[Constant]) -> Constant {
        unsafe {
            let l = args.len();
            let vec = create_vector(DT_ANY, 0);

            for i in 0..l {
                //	   s = append(s,C.CString(colname[i]));
                //		   s.Append(CreateString(colname[i]));
                //v = append(v, cols[i].ptr);
                vec.append(&args[i]);
            }
            return Constant {
                c: DBConnection_runfunc(self.conn, stringc(&script), vec.v),
            };
        }
    }
}

pub struct Table {
    t: *mut c_void,
}
impl Defobj for Table {
    fn obj_ptr(&self) -> *mut c_void {
        self.t
    }
}
impl Table {
    pub fn get_name(&self) -> String {
        unsafe {
            return stringr(Table_getName(self.t));
        }
    }
    pub fn get_column_name(&self, x: c_int) -> String {
        unsafe {
            return stringr(Table_getColumnName(self.t, x));
        }
    }
    pub fn get_column(&self, x: c_int) -> Vector {
        unsafe {
            return Vector {
                v: Table_getColumn(self.t, x),
            };
        }
    }
}

pub struct Vector {
    v: *mut c_void,
}
impl Defobj for Vector {
    fn obj_ptr(&self) -> *mut c_void {
        self.v
    }
}

impl Vector {
    pub fn get_name(&self) -> String {
        unsafe {
            return stringr(Vector_getName(self.v));
        }
    }

    pub fn get(&self, x: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Vector_get(self.v, x),
            };
        }
    }

    pub fn remove(&self, x: c_int) -> bool {
        unsafe {
            return Vector_remove(self.v, x);
        }
    }

    pub fn append(&self, x: &Constant) -> bool {
        unsafe {
            return Vector_append(self.v, x.c);
        }
    }

    pub fn set_name(&self, name: &str) {
        unsafe {
            Vector_setName(self.v, stringc(name));
        }
    }

    pub fn append_bool(&self, buf: &[bool], len: c_int) -> bool {
        unsafe {
            return Vector_appendBool(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_int(&self, buf: &[c_int], len: c_int) -> bool {
        unsafe {
            return Vector_appendInt(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_short(&self, buf: &[c_short], len: c_int) -> bool {
        unsafe {
            return Vector_appendShort(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_long(&self, buf: &[c_longlong], len: c_int) -> bool {
        unsafe {
            return Vector_appendLong(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_float(&self, buf: &[c_float], len: c_int) -> bool {
        unsafe {
            return Vector_appendFloat(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_double(&self, buf: &[c_double], len: c_int) -> bool {
        unsafe {
            return Vector_appendDouble(self.v, buf.as_ptr(), len);
        }
    }

    pub fn append_string(&self, buf: &[&str], len: c_int) -> bool {
        unsafe {
            for i in 0..(len as usize) {
                Vector_appendString(self.v, stringc(buf[i]), 1);
            }
            return true;
        }
    }
}
pub struct Matrix {
    m: *mut c_void,
}
impl Defobj for Matrix {
    fn obj_ptr(&self) -> *mut c_void {
        self.m
    }
}
impl Matrix {
    pub fn set_row_label(&self, label: Constant) {
        unsafe {
            Matrix_setRowLabel(self.m, label.c);
        }
    }

    pub fn set_column_label(&self, label: Constant) {
        unsafe {
            Matrix_setColumnLabel(self.m, label.c);
        }
    }

    pub fn reshape(&self, cols: c_int, rows: c_int) -> bool {
        unsafe {
            return Matrix_reshape(self.m, cols, rows);
        }
    }

    pub fn get_column(&self, index: c_int) -> Vector {
        unsafe {
            return Vector {
                v: Matrix_getColumn(self.m, index),
            };
        }
    }

    pub fn get_string_by_index(&self, index: c_int) -> String {
        unsafe {
            return stringr(Matrix_getStringbyIndex(self.m, index));
        }
    }

    pub fn get_cell_string(&self, col: c_int, row: c_int) -> String {
        unsafe {
            return stringr(Matrix_getCellString(self.m, col, row));
        }
    }

    pub fn set_column(&self, index: c_int, col: Vector) -> bool {
        unsafe {
            return Matrix_setColumn(self.m, index, col.v);
        }
    }

    pub fn get_instance(&self, size: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Matrix_getInstance(self.m, size),
            };
        }
    }

    /*
       pub fn get_name(&self) -> String
       {
        unsafe
        {
           return stringr(Vector_getName(self.v));
        }
      }

       pub fn get(&self, x: c_int) -> Constant
       {
        unsafe
        {
           return Constant{c:Vector_get(self.v, x)};
        }
      }
    */
}
pub struct Set {
    s: *mut c_void,
}
impl Defobj for Set {
    fn obj_ptr(&self) -> *mut c_void {
        self.s
    }
}
impl Set {
    pub fn clear(&self) {
        unsafe {
            Set_clear(self.s);
        }
    }

    pub fn remove(&self, val: Constant) -> bool {
        unsafe {
            return Set_remove(self.s, val.c);
        }
    }

    pub fn append(&self, val: Constant) -> bool {
        unsafe {
            return Set_append(self.s, val.c);
        }
    }

    pub fn inverse(&self, val: Constant) -> bool {
        unsafe {
            return Set_inverse(self.s, val.c);
        }
    }

    pub fn contain(&self, target: Constant, result: &Constant) {
        unsafe {
            Set_contain(self.s, target.c, result.c);
        }
    }

    pub fn is_super_set(&self, target: Constant) -> bool {
        unsafe {
            return Set_isSuperSet(self.s, target.c);
        }
    }

    pub fn get_script(&self) -> String {
        unsafe {
            //let r_str = CStr::from_ptr(Constant_getString(self.v));
            return stringr(Set_getScript(self.s));
        }
    }

    pub fn interaction(&self, w: Constant) -> Constant {
        unsafe {
            return Constant {
                c: Set_interaction(self.s, w.c),
            };
        }
    }

    pub fn get_sub_vector(&self, start: c_int, len: c_int) -> Vector {
        unsafe {
            return Vector {
                v: Set_getSubVector(self.s, start, len),
            };
        }
    }
}

pub struct Dictionary {
    d: *mut c_void,
}
impl Defobj for Dictionary {
    fn obj_ptr(&self) -> *mut c_void {
        self.d
    }
}

impl Dictionary {
    pub fn count(&self) -> c_int {
        unsafe {
            return Dictionary_count(self.d);
        }
    }

    pub fn clear(&self) {
        unsafe {
            Dictionary_clear(self.d);
        }
    }

    pub fn get_member(&self, key: Constant) -> Constant {
        unsafe {
            return Constant {
                c: Dictionary_getMember(self.d, key.c),
            };
        }
    }
    pub fn get_member_by_string(&self, key: &str) -> Constant {
        unsafe {
            return Constant {
                c: Dictionary_getMemberbyString(self.d, stringc(&key)),
            };
        }
    }
    pub fn get_cell(&self, col: c_int, row: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Dictionary_getCell(self.d, col, row),
            };
        }
    }

    pub fn get_key_type(&self) -> c_int {
        unsafe {
            return Dictionary_count(self.d);
        }
    }

    pub fn keys(&self) -> Constant {
        unsafe {
            return Constant {
                c: Dictionary_keys(self.d),
            };
        }
    }
    pub fn values(&self) -> Constant {
        unsafe {
            return Constant {
                c: Dictionary_values(self.d),
            };
        }
    }

    pub fn get_script(&self) -> String {
        unsafe {
            return stringr(Dictionary_getScript(self.d));
        }
    }

    pub fn remove(&self, key: Constant) -> bool {
        unsafe {
            return Dictionary_remove(self.d, key.c);
        }
    }

    pub fn set(&self, key: Constant, value: Constant) -> bool {
        unsafe {
            return Dictionary_set(self.d, key.c, value.c);
        }
    }

    pub fn set_by_string(&self, key: &str, value: Constant) -> bool {
        unsafe {
            return Dictionary_setbyString(self.d, stringc(key), value.c);
        }
    }

    pub fn contain(&self, target: Constant, result: Constant) {
        unsafe {
            Dictionary_contain(self.d, target.c, result.c);
        }
    }
}

impl Table {
    pub fn set_name(&self, name: &str) {
        unsafe {
            Table_setName(self.t, stringc(name));
        }
    }

    pub fn get_column_by_name(&self, name: &str) -> Vector {
        unsafe {
            return Vector {
                v: Table_getColumnbyName(self.t, stringc(name)),
            };
        }
    }

    pub fn columns(&self) -> c_int {
        unsafe {
            return Table_columns(self.t);
        }
    }

    pub fn rows(&self) -> c_int {
        unsafe {
            return Table_rows(self.t);
        }
    }

    pub fn get_column_type(&self, index: c_int) -> c_int {
        unsafe {
            return Table_getColumnType(self.t, index);
        }
    }
}

pub fn create_date(year: c_int, month: c_int, day: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createDate(year, month, day),
        };
    }
}

pub fn create_month(year: c_int, month: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createMonth(year, month),
        };
    }
}

pub fn create_nanotime(hour: c_int, minute: c_int, second: c_int, nanosecond: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createNanoTime(hour, minute, second, nanosecond),
        };
    }
}

pub fn create_time(hour: c_int, minute: c_int, second: c_int, millisecond: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createTime(hour, minute, second, millisecond),
        };
    }
}

pub fn create_second(hour: c_int, minute: c_int, second: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createSecond(hour, minute, second),
        };
    }
}

pub fn create_minute(hour: c_int, minute: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createMinute(hour, minute),
        };
    }
}

pub fn create_nanotimestamp(
    year: c_int,
    month: c_int,
    day: c_int,
    hour: c_int,
    minute: c_int,
    second: c_int,
    nanosecond: c_int,
) -> Constant {
    unsafe {
        return Constant {
            c: createNanoTimestamp(year, month, day, hour, minute, second, nanosecond),
        };
    }
}

pub fn create_timestamp(
    year: c_int,
    month: c_int,
    day: c_int,
    hour: c_int,
    minute: c_int,
    second: c_int,
    millisecond: c_int,
) -> Constant {
    unsafe {
        return Constant {
            c: createTimestamp(year, month, day, hour, minute, second, millisecond),
        };
    }
}

pub fn create_datetime(
    year: c_int,
    month: c_int,
    day: c_int,
    hour: c_int,
    minute: c_int,
    second: c_int,
) -> Constant {
    unsafe {
        return Constant {
            c: createDateTime(year, month, day, hour, minute, second),
        };
    }
}

pub fn create_int(x: c_int) -> Constant {
    unsafe {
        return Constant { c: createInt(x) };
    }
}
pub fn create_short(x: c_short) -> Constant {
    unsafe {
        return Constant { c: createShort(x) };
    }
}
pub fn create_long(x: c_longlong) -> Constant {
    unsafe {
        return Constant { c: createLong(x) };
    }
}
pub fn create_float(x: c_float) -> Constant {
    unsafe {
        return Constant { c: createFloat(x) };
    }
}
pub fn create_double(x: c_double) -> Constant {
    unsafe {
        return Constant { c: createDouble(x) };
    }
}
pub fn create_char(x: c_char) -> Constant {
    unsafe {
        return Constant { c: createChar(x) };
    }
}
pub fn create_bool(x: bool) -> Constant {
    unsafe {
        return Constant { c: createBool(x) };
    }
}
pub fn create_string(x: &str) -> Constant {
    unsafe {
        return Constant {
            c: createString(stringc(&x)),
        };
    }
}

pub fn del_constant(con: Constant) {
    unsafe {
        delConstant(con.c);
    }
}

pub fn create_vector(dttype: c_int, size: c_int) -> Vector {
    unsafe {
        return Vector {
            v: createVector(dttype, size),
        };
    }
}

pub fn create_table_by_vector(colname: &[&str], cols: &[Vector]) -> Table {
    unsafe {
        let l = colname.len();
        let colnamevec = create_vector(DT_STRING, 0);
        let colvec = create_vector(DT_ANY, 0);
        colnamevec.append_string(&colname[..], l as c_int);
        for i in 0..l {
            colvec.append(&cols[i].to_constant());
        }
        return Table {
            t: createTableByVector(colnamevec.v, colvec.v, l as c_int),
        };
    }
}

pub fn create_table(colname: &[&str], coltype: &[c_int], size: c_int, capacity: c_int) -> Table {
    unsafe {
        let l = colname.len();
        let colnamevec = create_vector(DT_STRING, 0);
        let coltypevec = create_vector(DT_INT, 0);

        colnamevec.append_string(&colname[..], l as c_int);
        coltypevec.append_int(&coltype[..], l as c_int);

        return Table {
            t: createTable(colnamevec.v, coltypevec.v, size, capacity, l as c_int),
        };
        //  return Table{v:createVector(dttype, size)}
    }
}

pub fn create_set(keytype: c_int, capacity: c_int) -> Set {
    unsafe {
        return Set {
            s: createSet(keytype, capacity),
        };
    }
}

pub fn create_dictionary(keytype: c_int, valuetype: c_int) -> Dictionary {
    unsafe {
        return Dictionary {
            d: createDictionary(keytype, valuetype),
        };
    }
}

impl Vector {
    pub fn get_column_label(&self) -> Constant {
        unsafe {
            return Constant {
                c: Vector_getColumnLabel(self.v),
            };
        }
    }

    pub fn is_view(&self) -> bool {
        unsafe {
            return Vector_isView(self.v);
        }
    }

    pub fn initialize(&self) {
        unsafe {
            Vector_initialize(self.v);
        }
    }

    pub fn get_capacity(&self) -> c_int {
        unsafe {
            return Vector_getCapacity(self.v);
        }
    }

    pub fn reserve(&self, capacity: c_int) -> c_int {
        unsafe {
            return Vector_reserve(self.v, capacity);
        }
    }

    pub fn get_unit_length(&self) -> c_int {
        unsafe {
            return Vector_getUnitLength(self.v);
        }
    }

    pub fn clear(&self) {
        unsafe {
            Vector_clear(self.v);
        }
    }

    pub fn remove_by_index(&self, index: Constant) -> bool {
        unsafe {
            return Vector_removebyIndex(self.v, index.c);
        }
    }

    pub fn get_instance(&self, size: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Vector_getInstance(self.v, size),
            };
        }
    }

    pub fn get_sub_vector(&self, start: c_int, len: c_int) -> Vector {
        unsafe {
            return Vector {
                v: Vector_getSubVector(self.v, start, len),
            };
        }
    }

    pub fn fill(&self, start: c_int, len: c_int, val: Constant) {
        unsafe {
            Vector_fill(self.v, start, len, val.c);
        }
    }

    pub fn next(&self, steps: c_int) {
        unsafe {
            Vector_next(self.v, steps);
        }
    }

    pub fn prev(&self, steps: c_int) {
        unsafe {
            Vector_prev(self.v, steps);
        }
    }

    pub fn reverse(&self) {
        unsafe {
            Vector_reverse(self.v);
        }
    }

    pub fn reverse_segment(&self, start: c_int, len: c_int) {
        unsafe {
            Vector_reverseSegment(self.v, start, len);
        }
    }
    pub fn replace(&self, oldval: Constant, newval: Constant) {
        unsafe {
            Vector_replace(self.v, oldval.c, newval.c);
        }
    }

    pub fn valid_index(&self, index: c_int) -> bool {
        unsafe {
            return Vector_validIndex(self.v, index);
        }
    }

    pub fn add_index(&self, start: c_int, len: c_int, offset: c_int) {
        unsafe {
            Vector_addIndex(self.v, start, len, offset);
        }
    }

    pub fn neg(&self) {
        unsafe { Vector_neg(self.v) }
    }
}

impl Table {
    pub fn get_script(&self) -> String {
        unsafe {
            return stringr(Table_getScript(self.t));
        }
    }

    pub fn get_column_qualifier(&self, index: c_int) -> String {
        unsafe {
            return stringr(Table_getColumnQualifier(self.t, index));
        }
    }

    pub fn set_column_name(&self, index: c_int, name: &str) {
        unsafe {
            Table_setColumnName(self.t, index, stringc(name));
        }
    }

    pub fn get_column_index(&self, name: &str) -> c_int {
        unsafe {
            return Table_getColumnIndex(self.t, stringc(name));
        }
    }

    pub fn contain(&self, name: &str) -> bool {
        unsafe {
            return Table_contain(self.t, stringc(name));
        }
    }

    pub fn get_value(&self) -> Constant {
        unsafe {
            return Constant {
                c: Table_getValue(self.t),
            };
        }
    }

    pub fn get_instance(&self, size: c_int) -> Constant {
        unsafe {
            return Constant {
                c: Table_getInstance(self.t, size),
            };
        }
    }

    pub fn sizeable(&self) -> bool {
        unsafe {
            return Table_sizeable(self.t);
        }
    }

    pub fn get_string_by_index(&self, index: c_int) -> String {
        unsafe {
            return stringr(Table_getStringbyIndex(self.t, index));
        }
    }

    pub fn get_window(
        &self,
        colstart: c_int,
        collen: c_int,
        rowstart: c_int,
        rowlen: c_int,
    ) -> Constant {
        unsafe {
            return Constant {
                c: Table_getWindow(self.t, colstart, collen, rowstart, rowlen),
            };
        }
    }

    pub fn get_member(&self, key: Constant) -> Constant {
        unsafe {
            return Constant {
                c: Table_getMember(self.t, key.c),
            };
        }
    }

    pub fn values(&self) -> Constant {
        unsafe {
            return Constant {
                c: Table_values(self.t),
            };
        }
    }

    pub fn keys(&self) -> Constant {
        unsafe {
            return Constant {
                c: Table_keys(self.t),
            };
        }
    }

    pub fn get_table_type(&self) -> c_int {
        unsafe {
            return Table_getTableType(self.t);
        }
    }
}

#[derive(Debug)]
pub struct MessageQueue {
    m: *mut c_void,
}

#[derive(Debug)]
pub struct PollingClient {
    p: *mut c_void,
}

//fn set_Int_slice(&self, start: c_int, len:c_int, buf:&[c_int]) -> bool {
//  unsafe{
//    return Constant_setIntArray(self.obj_ptr(), start, len, buf.as_ptr());
//  }
// }
impl Constant {
    pub fn new() -> Constant {
        unsafe {
            return Constant { c: Constant_new() };
        }
    }

    pub fn set_binary(&self, buf: &[c_uchar]) {
        unsafe {
            Constant_setBinary(self.obj_ptr(), buf.as_ptr());
        }
    }

    pub fn get_hash(&self, buckets: c_int) -> c_int {
        unsafe {
            return Constant_getHash(self.obj_ptr(), buckets);
        }
    }
}

pub fn create_constant(dt_type: c_int) -> Constant {
    unsafe {
        return Constant {
            c: createConstant(dt_type),
        };
    }
}

pub fn parse_constant(dt_type: c_int, val: &str) -> Constant {
    unsafe {
        return Constant {
            c: parseConstant(dt_type, stringc(val)),
        };
    }
}

pub fn get_epoch_time() -> c_longlong {
    unsafe {
        return getEpochTime();
    }
}
/*
fn set_String(&self, val: &str){
   unsafe{
    Constant_setString(self.obj_ptr(), stringc(val));
 }
 }
 */
pub fn default_action_name() -> String {
    unsafe {
        return stringr(def_action_name());
    }
}
impl MessageQueue {
    pub fn poll(&self, msg: &Constant, sec: c_int) -> bool {
        unsafe {
            return MessageQueue_poll(self.m, msg.obj_ptr(), sec);
        }
    }
}

impl PollingClient {
    pub fn new(listerport: c_int) -> PollingClient {
        unsafe {
            return PollingClient {
                p: PollingClient_new(listerport),
            };
        }
    }

    pub fn subscribe(
        &self,
        host: &str,
        port: c_int,
        table_name: &str,
        action_name: &str,
        offset: c_longlong,
    ) -> MessageQueue {
        unsafe {
            return MessageQueue {
                m: PollingClient_subscribe(
                    self.p,
                    stringc(host),
                    port,
                    stringc(table_name),
                    stringc(action_name),
                    offset,
                ),
            };
        }
    }

    pub fn unsubscribe(
        &self,
        host: &str,
        port: c_int,
        table_name: &str,
        action_name: &str,

    ) {
        unsafe {
            PollingClient_unsubscribe(
                self.p,
                stringc(host),
                port,
                stringc(table_name),
                stringc(action_name),
            );
        }
    }
}
extern "C" {

    //fn Constant_setIntArray(w: *mut c_void, start: c_int, len: c_int, buf: *const c_int) -> bool;
    fn Constant_setBinary(w: *mut c_void, val: *const c_uchar);
    fn Constant_setBinaryByIndex(w: *mut c_void, index: c_int, val: *const c_uchar);
    fn Constant_setBinaryArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        val: *const c_uchar,
    ) -> bool;

    fn createConstant(dt_type: c_int) -> *mut c_void;
    fn parseConstant(dt_type: c_int, word: *mut c_char) -> *mut c_void;
    fn Constant_getHash(c: *mut c_void, buckets: c_int) -> c_int;
    fn getEpochTime() -> c_longlong;
    fn MessageQueue_poll(m: *mut c_void, msg: *const c_void, s: c_int) -> bool;
    fn def_action_name() -> *mut c_char;
    fn PollingClient_new(listerport: c_int) -> *mut c_void;
    fn PollingClient_subscribe(
        client: *mut c_void,
        host: *mut c_char,
        port: c_int,
        tableName: *mut c_char,
        actionName: *mut c_char,
        offset: c_longlong,
    ) -> *mut c_void;
    fn PollingClient_unsubscribe(
        client: *mut c_void,
        host: *mut c_char,
        port: c_int,
        tableName: *mut c_char,
        actionName: *mut c_char,
    );

    fn DBConnection_new() -> *mut c_void;
    fn DBConnection_connect(
        conn: *mut c_void,
        host: *mut c_char,
        port: c_int,
        user: *mut c_char,
        pass: *mut c_char,
    ) -> bool;
    fn DBConnection_run(conn: *mut c_void, s: *mut c_char) -> *mut c_void;
    fn DBConnection_close(conn: *mut c_void);
    fn DBConnection_upload(conn: *mut c_void, name: *mut c_char, c: *mut c_void);

    fn Constant_size(c: *mut c_void) -> c_int;

    fn Constant_getInt(c: *mut c_void) -> c_int;
    fn Constant_getBool(c: *mut c_void) -> bool;
    fn Constant_getShort(c: *mut c_void) -> c_short;
    fn Constant_getLong(c: *mut c_void) -> c_longlong;
    fn Constant_getChar(c: *mut c_void) -> c_char;
    fn Constant_getFloat(c: *mut c_void) -> c_float;
    fn Constant_getDouble(c: *mut c_void) -> c_double;
    fn Constant_getIndex(c: *mut c_void) -> c_int;
    fn Constant_getString(c: *mut c_void) -> *mut c_char;

    fn Constant_isScalar(c: *mut c_void) -> bool;
    fn Constant_isPair(c: *mut c_void) -> bool;
    fn Constant_isMatrix(c: *mut c_void) -> bool;
    fn Constant_isVector(c: *mut c_void) -> bool;
    fn Constant_isTable(c: *mut c_void) -> bool;
    fn Constant_isSet(c: *mut c_void) -> bool;
    fn Constant_isDictionary(c: *mut c_void) -> bool;

    fn Constant_getForm(c: *mut c_void) -> c_int;
    fn Constant_getType(c: *mut c_void) -> c_int;

    fn toConstant(c: *mut c_void) -> *mut c_void;
    fn toTable(c: *mut c_void) -> *mut c_void;
    fn toMatrix(c: *mut c_void) -> *mut c_void;
    fn toVector(c: *mut c_void) -> *mut c_void;
    fn toSet(c: *mut c_void) -> *mut c_void;
    fn toDictionary(c: *mut c_void) -> *mut c_void;

    fn Table_getName(c: *mut c_void) -> *mut c_char;
    fn Table_getColumnName(c: *mut c_void, x: c_int) -> *mut c_char;
    fn Table_getColumn(c: *mut c_void, x: c_int) -> *mut c_void;

    fn Vector_getName(c: *mut c_void) -> *mut c_char;
    fn Vector_get(c: *mut c_void, x: c_int) -> *mut c_void;
    fn Vector_remove(c: *mut c_void, x: c_int) -> bool;
    fn Vector_append(c: *mut c_void, x: *mut c_void) -> bool;

    fn createInt(x: c_int) -> *mut c_void;
    fn createShort(x: c_short) -> *mut c_void;
    fn createLong(x: c_longlong) -> *mut c_void;
    fn createChar(x: c_char) -> *mut c_void;
    fn createFloat(x: c_float) -> *mut c_void;
    fn createDouble(x: c_double) -> *mut c_void;
    fn createBool(x: bool) -> *mut c_void;
    fn createString(x: *mut c_char) -> *mut c_void;

    // fn DBConnection_new() -> DBConnection;
    //  fn DBConnection_connect(conn:&DBConnection, host: *mut c_char, port: c_int, user:*mut c_char,pass: *mut c_char) -> c_int;

    fn createVector(dttype: c_int, size: c_int) -> *mut c_void;

    fn Set_clear(c: *mut c_void);
    fn Set_remove(setptr: *mut c_void, cptr: *mut c_void) -> bool;
    fn Set_append(setptr: *mut c_void, cptr: *mut c_void) -> bool;
    fn Set_inverse(setptr: *mut c_void, cptr: *mut c_void) -> bool;
    fn Set_contain(setptr: *mut c_void, target: *mut c_void, result: *const c_void);
    fn Set_isSuperSet(setptr: *mut c_void, target: *mut c_void) -> bool;
    fn Set_getScript(c: *mut c_void) -> *mut c_char;

    fn Constant_isLargeConstant(c: *mut c_void) -> bool;

    fn Set_interaction(setptr: *mut c_void, target: *mut c_void) -> *mut c_void;
    fn Set_getSubVector(setptr: *mut c_void, start: c_int, length: c_int) -> *mut c_void;

    fn Matrix_setRowLabel(mptr: *mut c_void, label: *mut c_void);
    fn Matrix_setColumnLabel(mptr: *mut c_void, label: *mut c_void);
    fn Matrix_reshape(mptr: *mut c_void, cols: c_int, rows: c_int) -> bool;
    fn Matrix_getColumn(mptr: *mut c_void, index: c_int) -> *mut c_void;
    fn Matrix_getStringbyIndex(mptr: *mut c_void, index: c_int) -> *mut c_char;
    fn Matrix_getCellString(mptr: *mut c_void, col: c_int, row: c_int) -> *mut c_char;
    fn Matrix_setColumn(mptr: *mut c_void, index: c_int, col: *mut c_void) -> bool;
    fn Matrix_getInstance(mptr: *mut c_void, size: c_int) -> *mut c_void;

    fn Dictionary_count(dptr: *mut c_void) -> c_int;
    fn Dictionary_clear(dptr: *mut c_void);
    fn Dictionary_getMember(dptr: *mut c_void, key: *mut c_void) -> *mut c_void;
    fn Dictionary_getMemberbyString(dptr: *mut c_void, key: *mut c_char) -> *mut c_void;
    fn Dictionary_getCell(mptr: *mut c_void, col: c_int, row: c_int) -> *mut c_void;
    //fn Dictionary_getKeyType(dptr: *mut c_void) -> c_int;
    fn Dictionary_keys(dptr: *mut c_void) -> *mut c_void;
    fn Dictionary_values(dptr: *mut c_void) -> *mut c_void;
    fn Dictionary_getScript(dptr: *mut c_void) -> *mut c_char;
    fn Dictionary_remove(dptr: *mut c_void, key: *mut c_void) -> bool;
    fn Dictionary_set(dptr: *mut c_void, key: *mut c_void, value: *mut c_void) -> bool;
    fn Dictionary_setbyString(dptr: *mut c_void, key: *mut c_char, value: *mut c_void) -> bool;
    fn Dictionary_contain(dptr: *mut c_void, target: *mut c_void, result: *mut c_void);

    fn Table_setName(t: *mut c_void, name: *mut c_char);
    fn Table_getColumnbyName(t: *mut c_void, name: *mut c_char) -> *mut c_void;
    fn Table_columns(t: *mut c_void) -> c_int;
    fn Table_rows(t: *mut c_void) -> c_int;
    fn Table_getColumnType(t: *mut c_void, index: c_int) -> c_int;

    fn createDate(year: c_int, month: c_int, day: c_int) -> *mut c_void;
    fn createMonth(year: c_int, month: c_int) -> *mut c_void;
    fn createNanoTime(hour: c_int, minute: c_int, second: c_int, nanosecond: c_int) -> *mut c_void;
    fn createTime(hour: c_int, minute: c_int, second: c_int, millisecond: c_int) -> *mut c_void;
    fn createSecond(hour: c_int, minute: c_int, second: c_int) -> *mut c_void;
    fn createMinute(hour: c_int, minute: c_int) -> *mut c_void;
    fn createNanoTimestamp(
        year: c_int,
        month: c_int,
        day: c_int,
        hour: c_int,
        minute: c_int,
        second: c_int,
        nanosecond: c_int,
    ) -> *mut c_void;
    fn createTimestamp(
        year: c_int,
        month: c_int,
        day: c_int,
        hour: c_int,
        minute: c_int,
        second: c_int,
        millisecond: c_int,
    ) -> *mut c_void;
    fn createDateTime(
        year: c_int,
        month: c_int,
        day: c_int,
        hour: c_int,
        minute: c_int,
        second: c_int,
    ) -> *mut c_void;

    fn Vector_setName(w: *mut c_void, vname: *mut c_char);
    fn Constant_get(w: *mut c_void, x: c_int) -> *mut c_void;

    fn Vector_appendBool(c: *mut c_void, buf: *const bool, len: c_int) -> bool;
    fn Vector_appendInt(c: *mut c_void, buf: *const c_int, len: c_int) -> bool;
    fn Vector_appendShort(c: *mut c_void, buf: *const c_short, len: c_int) -> bool;
    fn Vector_appendLong(c: *mut c_void, buf: *const c_longlong, len: c_int) -> bool;
    fn Vector_appendFloat(c: *mut c_void, buf: *const c_float, len: c_int) -> bool;
    fn Vector_appendDouble(c: *mut c_void, buf: *const c_double, len: c_int) -> bool;
    fn Vector_appendString(c: *mut c_void, buf: *const c_char, len: c_int) -> bool;

    fn Vector_getColumnLabel(w: *mut c_void) -> *mut c_void;
    fn Vector_isView(v: *mut c_void) -> bool;
    fn Vector_initialize(v: *mut c_void);
    fn Vector_getCapacity(v: *mut c_void) -> c_int;
    fn Vector_reserve(v: *mut c_void, capacity: c_int) -> c_int;
    fn Vector_getUnitLength(v: *mut c_void) -> c_int;
    fn Vector_clear(v: *mut c_void);
    fn Vector_removebyIndex(v: *mut c_void, index: *mut c_void) -> bool;
    fn Vector_getInstance(v: *mut c_void, size: c_int) -> *mut c_void;
    fn Vector_getSubVector(v: *mut c_void, start: c_int, len: c_int) -> *mut c_void;
    fn Vector_fill(w: *mut c_void, start: c_int, l: c_int, val: *mut c_void);
    fn Vector_next(w: *mut c_void, steps: c_int);
    fn Vector_prev(w: *mut c_void, steps: c_int);
    fn Vector_reverse(w: *mut c_void);
    fn Vector_reverseSegment(w: *mut c_void, start: c_int, len: c_int);
    fn Vector_replace(w: *mut c_void, old_val: *mut c_void, new_val: *mut c_void);
    fn Vector_validIndex(w: *mut c_void, index: c_int) -> bool;
    fn Vector_addIndex(w: *mut c_void, start: c_int, len: c_int, offset: c_int);
    fn Vector_neg(w: *mut c_void);

    fn Table_getScript(w: *mut c_void) -> *mut c_char;
    fn Table_getColumnQualifier(w: *mut c_void, index: c_int) -> *mut c_char;
    fn Table_setColumnName(w: *mut c_void, index: c_int, name: *mut c_char);
    fn Table_getColumnIndex(w: *mut c_void, name: *mut c_char) -> c_int;
    fn Table_contain(w: *mut c_void, name: *mut c_char) -> bool;
    fn Table_getValue(w: *mut c_void) -> *mut c_void;
    fn Table_getInstance(w: *mut c_void, size: c_int) -> *mut c_void;
    fn Table_sizeable(w: *mut c_void) -> bool;
    fn Table_getStringbyIndex(w: *mut c_void, index: c_int) -> *mut c_char;
    fn Table_getWindow(
        w: *mut c_void,
        colstart: c_int,
        collen: c_int,
        rowstart: c_int,
        rowlen: c_int,
    ) -> *mut c_void;
    fn Table_getMember(w: *mut c_void, key: *mut c_void) -> *mut c_void;
    fn Table_values(w: *mut c_void) -> *mut c_void;
    fn Table_keys(w: *mut c_void) -> *mut c_void;
    fn Table_getTableType(w: *mut c_void) -> c_int;
   // fn Table_drop(w: *mut c_void, v: *mut c_void);

    fn Constant_setBoolArray(w: *mut c_void, start: c_int, len: c_int, buf: *const bool) -> bool;
    fn Constant_setIntArray(w: *mut c_void, start: c_int, len: c_int, buf: *const c_int) -> bool;
    fn Constant_setLongArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        buf: *const c_longlong,
    ) -> bool;
    fn Constant_setShortArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        buf: *const c_short,
    ) -> bool;
    fn Constant_setFloatArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        buf: *const c_float,
    ) -> bool;
    fn Constant_setDoubleArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        buf: *const c_double,
    ) -> bool;
    fn Constant_setStringArray(
        w: *mut c_void,
        start: c_int,
        len: c_int,
        buf: *const c_char,
    ) -> bool;

    fn Constant_setIntByIndex(w: *mut c_void, index: c_int, val: c_int);
    fn Constant_setBoolByIndex(w: *mut c_void, index: c_int, val: bool);
    fn Constant_setShortByIndex(w: *mut c_void, index: c_int, val: c_short);
    fn Constant_setLongByIndex(w: *mut c_void, index: c_int, val: c_longlong);
    fn Constant_setFloatByIndex(w: *mut c_void, index: c_int, val: c_float);
    fn Constant_setDoubleByIndex(w: *mut c_void, index: c_int, val: c_double);
    fn Constant_setStringByIndex(w: *mut c_void, index: c_int, val: *const c_char);
    fn Constant_setNullByIndex(w: *mut c_void, index: c_int);
    fn Constant_setByIndex(w: *mut c_void, index: c_int, val: *mut c_void) -> bool;

    fn Constant_setInt(w: *mut c_void, val: c_int);
    fn Constant_setBool(w: *mut c_void, val: bool);
    fn Constant_setShort(w: *mut c_void, val: c_short);
    fn Constant_setLong(w: *mut c_void, val: c_longlong);
    fn Constant_setFloat(w: *mut c_void, val: c_float);
    fn Constant_setDouble(w: *mut c_void, val: c_double);
    fn Constant_setString(w: *mut c_void, val: *const c_char);
    fn Constant_setNull(w: *mut c_void);

    fn delConstant(w: *mut c_void);
    fn createTable(
        colname: *mut c_void,
        coltype: *mut c_void,
        size: c_int,
        capacity: c_int,
        len: c_int,
    ) -> *mut c_void;

    fn createTableByVector(colname: *mut c_void, cols: *mut c_void, len: c_int) -> *mut c_void;

    fn DBConnection_runfunc(
        conn: *mut c_void,
        script: *mut c_char,
        args: *mut c_void,
    ) -> *mut c_void;

    fn createSet(keytype: c_int, capacity: c_int) -> *mut c_void;
    fn createDictionary(keytype: c_int, valuetype: c_int) -> *mut c_void;

    fn Constant_isNull(c: *mut c_void) -> bool;
    fn Constant_new() -> *mut c_void;
//void delConstant(Wrapper* w)
//void* DBConnection_runfunc(DBConnection* conn, char* script, WrapperVector* args)
//void* createTable(WrapperVector* colname,WrapperVector* coltypes,int size, int capacity, int len)
// table drop

}

//TODO
//fn main() {
    /*
            let host = "localhost";
            let user = "admin";
            let pass = "123456";



      let y: DBConnection = DBConnection::new();
      // y::new();
     println!("{}",y.connect(host, 1621, user, pass));
    // let s = "select * from t";

    // let s = "1+2";
      //let x:Constant = y.run("1+1");
      let months = ["January", "February", "March", "April", "May", "June", "July",
                  "August", "September", "October", "November", "December"];

     //let shuzu: [str; 5] = ["123" ,"3124" , "3123", "asd", "!@$@!"];
      let x:Constant =  y.run("1+1");
      let z:Table = x.to_Table();
      let w:Vector = z.get_column(0);
        println!("{:#?}",w.get_string());
      let a:Constant = w.get(0);
      println!("{:#?}",a.get_int());
      let b:Constant =  w.to_Constant();
       println!("{:#?}",b.get_string());


      let one = 5;
      let f = create_Int(one);
      println!("{:#?}",f.get_string());

      let v1 = create_vector(4,0);
      println!("{}",v1.size());

    // println!("{:#?}", x.get_string());




      let arr1: [c_int;5] = [1,2,3,4,5];
      let slice = &arr1[..];


      v1.append_Int(&arr1[..],3);
      let str1 =  "123123";
      let strs: [&str;5] = ["123", "456","768","ased","dqdqwf"];
        println!("{}",strs[1]);

      println!("{:#?}",v1.get_string());



       let t1 =  y.run("table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x)");

       println!("{}",t1.get_string());



     //  println!("{:#?}",x.get_type());
     // println!("{}",x);
     //println!("{}",x.getInt());
    //  y.close();
    //   let s = CString::new(String::from("101+1")).unwrap();
    // y.run(s.into_raw());
    //   let _x:*mut c_void =  DBConnection_new();
    //   let t: c_int = DBConnection_connect( _x, host.into_raw(), 1621, user.into_raw(), pass.into_raw());
     //   println!("{}",t);

     //    let s = CString::new(String::from("101+1")).unwrap();
     //   println!("{}",DBConnection_run(_x, s.into_raw()));


     //   let host: *mut c_char = "localhost";
      //  let t: c_int = DBConnection_connect( _x, "localhost", 1621, "admin", "123456");

     //   let input = 4;
    //    let output = unsafe { double_input(input) };
     //   println!("{} * 2 = {}", input, output);
     */
//}
