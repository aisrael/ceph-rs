extern crate ceph;

use std::env;

use ceph::rados::version;
use ceph::rados::Cluster;

fn main() {
	let args: Vec<_> = env::args().collect();
    println!("{}: rados::version() => {}", args[0], ceph::rados::version());

    /* Initialize the cluster handle with a 'null' cluster name and the "client.admin" user */
	let cluster = Cluster::create(None, "client.admin", 0).unwrap_or_else(|e|
		panic!(format!("{}: Couldn't create the cluster handle! {}", args[0], e))
	);
    println!("Created a cluster handle: {:?}", cluster);

    /* Read a Ceph configuration file to configure the cluster handle. */
    cluster.conf_read_file("ceph.conf").unwrap_or_else(|e|
		panic!(format!("{}: cannot read config file {}", args[0], e))
    );
    println!("Read the config file");

    /* Read command line arguments */
    cluster.conf_parse_argv(&args).unwrap_or_else(|e|
		panic!(format!("{}: cannot parse command line arguments {}", args[0], e))
	);
    println!("Read the command line arguments.");

    /* Connect to the cluster */
    cluster.connect().unwrap_or_else(|e|
        panic!(format!("{}: cannot connect to cluster: {}", args[0], e))
	);
    println!("Connected to the cluster");

    let fsid = cluster.fsid().unwrap_or_else(|e|
        panic!(format!("{}: cannot get clusted fsid: {}", args[0], e))
    );
    println!("Cluster FSID: {}", fsid);

    let poolname = "data";
    let ioctx = cluster.create_ioctx(poolname).unwrap_or_else(|e|
        panic!(format!("{}: cannot open rados pool: {}", args[0], e))
	);
    println!("Created I/O context.");

    /* Write data to the cluster synchronously. */
    let oid = "hw";
    let data = "Hello, world.";
    println!("Setting \"{}\" to \"{}\"", oid, data);
    ioctx.write(oid, data).unwrap_or_else(|e|
    	panic!(format!("{}: Cannot write object \"{}\" to pool {}: {}", args[0], oid, poolname, e))
	);
    println!("Wrote \"{}\" to object \"{}\".", data, oid);

    let xattr_key = "lang";
    let xattr_value = "en_US";
    ioctx.setxattr(oid, xattr_key, xattr_value).unwrap_or_else(|e|
        panic!(format!("{}: Cannot write xattr to pool {}: {}", args[0], poolname, e))
    );
    println!("Wrote \"{}\" to xattr \"{}\" for object \"{}\".", xattr_value, xattr_key, oid);

    let read = ioctx.read("hw", data.len()).unwrap_or_else(|e|
        panic!(format!("{}: Cannot read object \"{}\" from pool {}: {}", args[0], oid, poolname, e))
    );
    println!("Read object {} => \"{}\"", oid, read);

    let xattr_read = ioctx.getxattr(oid, xattr_key, 5).unwrap_or_else(|e|
        panic!(format!("{}: Cannot read xattr \"{}\" from pool {}: {}", args[0], xattr_key, poolname, e))
    );
    println!("Read xattr \"{}\" for object \"{}\". The contents are: \"{}\"", xattr_key, oid, xattr_read);

    ioctx.remove(oid).unwrap_or_else(|e|
    	panic!(format!("{}: Cannot remove object \"{}\" from pool {}: {}", args[0], oid, poolname, e))
	);
    println!("Removed object \"{}\".", oid);

}
