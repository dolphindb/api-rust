use dolphindb::types::*;

fn main() {
    // create empty data forms
    let _v = Vector::<Int>::new();
    let _v = Vector::<Int>::with_capacity(1);
    let _s = Set::<Int>::new();
    let _s = Set::<Int>::with_capacity(1);
    // Value type of dictionary is ANY
    let _d = Dictionary::<Int>::new();
    let _d = Dictionary::<Int>::with_capacity(1);

    let _v = Vector::<Int>::from_raw(&[1]);
    let _v = IntVector::from_raw(&[1]);

    let _a = IntArrayVector::new();

    // use data forms
    let mut v = IntVector::new();
    // Usage of dolphindb's Vector is similar to Vec<T>
    v.push(1.into());
    let _t = v[0];
    let mut s = Set::<Int>::new();
    // Usage of dolphindb's Set is similar to HashSet<T>
    s.insert(1.into());
    s.get(&Int::new(1));
    let mut d = Dictionary::<Int>::new();
    // Usage of dolphindb's Dictionary is similar to HashMap<T, Any>
    d.insert(1.into(), Int::new(2));
    d.insert(1.into(), Double::new(2.0));
    d.get(&Int::new(1));
    let mut a = IntArrayVector::new();
    a.push(vec![1, 2, 3]);
}
