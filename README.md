# ceph-rs

ceph-rs is an idiomatic wrapper around Ceph's `librados`.

###  Usage

```rust
extern crate ceph;

use std::env;

use ceph::rados::version;
use ceph::rados::Cluster;

fn main() {
    println!("rados::version() => {}", args[0], ceph::rados::version());

    /* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
	let cluster = Cluster::create("ceph", "client.admin", 0).unwrap_or_else(|e|
		panic!(format!("Couldn't create the cluster handle! {}", e))
	);
    println!("Created a cluster handle: {:?}", cluster);

    /* Read a Ceph configuration file to configure the cluster handle. */
    cluster.conf_read_file("ceph.conf").unwrap_or_else(|e|
		panic!(format!("Cannot read config file {}", e))
    );
    println!("Read the config file");

    /* Read command line arguments */
    cluster.conf_parse_argv(&args).unwrap_or_else(|e|
		panic!(format!("Cannot parse command line arguments {}", e))
	);
    println!("Read the command line arguments.");

    /* Connect to the cluster */
    cluster.connect().unwrap_or_else(|e|
        panic!(format!("Cannot connect to cluster: {}", e))
	);
    println!("Connected to the cluster");

    let ioctx = cluster.create_ioctx("data").unwrap_or_else(|e|
        panic!(format!("Cannot open rados pool: {}", e))
	);
    println!("Created I/O context.");

    /* Write data to the cluster synchronously. */
    let data = "Hello, World!";
    ioctx.write("hw", data).unwrap_or_else(|e|
    	panic!(format!("Cannot write object \"hw\" to pool \"data\": {}", key, e))
	);
    println!("Wrote \"{}\" to object \"hw\".", data);

    let read = ioctx.read(key, data.len()).unwrap_or_else(|e|
    	panic!(format!("Cannot read object \"hw\" from pool \"data\": {}", e))
	);
	println!("Read object \"hw\" => \"{}\"", read);

    ioctx.remove(key).unwrap_or_else(|e|
    	panic!(format!("Cannot remove object \"hw\" from pool \"data\": {}", e))
	);
    println!("Removed object \"hw\".");

}
```