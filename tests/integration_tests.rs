use std::{
    fs::{self, read_dir},
    path::Path,
};

fn clear_data() {
    let path = Path::new("data");
    dbg!(path.read_dir().unwrap().count());
    for file in read_dir(path).unwrap() {
        let path = file.unwrap().path();
        if path.extension().is_some() {
            fs::remove_file(&path).ok();
        }
    }
}
