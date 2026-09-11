use std::env;

use shepherd::greeting;

fn main() {
    let name = env::args().skip(1).collect::<Vec<_>>().join(" ");
    println!("{}", greeting(&name));
}
