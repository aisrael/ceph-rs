use std::iter::repeat;
use std::ffi::{CStr, CString};
use std::fmt;
use std::ptr;

use core::fmt::Debug;
use core::fmt::Formatter;

use libc::c_void;
use libc::c_char;
use libc::c_int;
use libc::size_t;
use libc::strerror;

#[allow(non_camel_case_types)]
type c_void_ptr = *const c_void;
#[allow(non_camel_case_types)]
type rados_t = c_void_ptr;
#[allow(non_camel_case_types)]
type rados_ioctx_t = c_void_ptr;

#[link(name = "rados")]
#[allow(dead_code)]
extern "C" {

	/// Get the version of librados.
	///
	/// The version number is `major.minor.extra`. Note that this is
	/// unrelated to the Ceph version number.
	///
	/// # Parameters
	///
	/// * `major` where to store the major version number
	/// * `minor` where to store the minor version number
	/// * `extra` where to store the extra version number
 	fn rados_version(major: *mut c_int, minor: *mut c_int, extra: *mut c_int);

	/// Create a handle for communicating with a RADOS cluster.
	///
	/// Ceph environment variables are read when this is called, so if
	/// $CEPH_ARGS specifies everything you need to connect, no further
	/// configuration is necessary.
	///
	/// # Parameters
	///
	/// * `cluster` where to store the handle
	/// * `id` the user to connect as (i.e. `"admin"`, not `"client.admin"`)
	/// * `0` on success, negative error code on failure
	fn rados_create(cluster: &rados_t, id: *const c_char) -> c_int;

	/// Extended version of rados_create.
	///
	/// Like rados_create, but
	/// 1) don't assume `"client." + id`; allow full specification of name
	/// 2) allow specification of cluster name
	/// 3) flags for future expansion
 	fn rados_create2(cluster: &rados_t, cluster_name: *const c_char,
		user_name: *const c_char, flags: u64) -> c_int;

	/// Connect to the cluster.
	///
	/// # Note
	///
	/// **BUG:** Before calling this, calling a function that communicates with the
	/// cluster will crash.
	///
	/// # Prerequisites
	///
	/// The cluster handle is configured with at least a monitor address.
	/// If cephx is enabled, a client name and secret must also be set.
	///
	/// # Post
	///
	/// If this succeeds, any function in librados may be used
	///
	/// # Parameters
	///
	/// * `cluster` The cluster to connect to.
	///
	/// # Returns
	///
	/// * 0 on sucess
	/// * negative error code on failure
	fn rados_connect(cluster: rados_t) -> c_int;

	/// Configure the cluster handle using a Ceph config file
	///
	/// If path is `NULL`, the default locations are searched, and the first
	/// found is used. The locations are:
	///
	/// * `$CEPH_CONF` (environment variable)
	/// * `/etc/ceph/ceph.conf`
	/// * `~/.ceph/config`
	/// * `ceph.conf` (in the current working directory)
	///
	/// # Prerequisites
	///
	/// `rados_connect()` has not been called on the cluster handle
	///
	/// # Parameters
	///
	/// * `cluster` cluster handle to configure
	/// * `path` path to a Ceph configuration file
	/// * 0 on success, negative error code on failure
 	fn rados_conf_read_file(cluster: rados_t, path: *const c_char) -> c_int;

	/// Configure the cluster handle with command line arguments
	///
	/// `argv` can contain any common Ceph command line option, including any
	/// configuration parameter prefixed by `--` and replacing spaces with
	/// dashes or underscores. For example, the following options are equivalent:
	///
	/// * `--mon-host 10.0.0.1:6789`
	/// * `--mon_host 10.0.0.1:6789`
	/// * `-m 10.0.0.1:6789`
	///
	/// # Prerequisites
	///
	/// `rados_connect()` has not been called on the cluster handle
	///
	/// # Parameters
	///
	/// * `cluster` cluster handle to configure
	/// * `argc` number of arguments in argv
	/// * `argv` arguments to parse
	/// * 0 on success, negative error code on failure
	fn rados_conf_parse_argv(cluster: rados_t, argc: c_int, argv: *const *const c_char) -> c_int;

	/// Get the fsid of the cluster as a hexadecimal string.
	///
	/// The fsid is a unique id of an entire Ceph cluster.
	///
	/// # Parameters
	///
	/// * `cluster` where to get the fsid
	/// * `buf` where to write the fsid
	/// * `len` the size of buf in bytes (should be 37)
	///
	/// # Returns
	///
	/// * `0` on success, negative error code on failure
	/// * `-ERANGE` if the buffer is too short to contain the fsid
	fn rados_cluster_fsid(cluster: rados_t, buf: *mut c_char, len: size_t) -> c_int;

	fn rados_ioctx_create(cluster: c_void_ptr, poolname: *const c_char, ioctx: &rados_ioctx_t) -> c_int;
	fn rados_write(io: rados_ioctx_t, oid: *const c_char, buf: *const c_char, len: size_t, offset: u64) -> c_int;

	/// Write *len* bytes from *buf* into the *oid* object. The value of
	/// *len* must be <= UINT_MAX/2.
	///
	/// The object is filled with the provided data. If the object exists,
	/// it is atomically truncated and then written.
	///
	/// @param io the io context in which the write will occur
	/// @param oid name of the object
	/// @param buf data to write
	/// @param len length of the data, in bytes
	/// @returns 0 on success, negative error code on failure
 	fn rados_write_full(io: rados_ioctx_t, oid: *const c_char, buf: *const c_char, len: size_t) -> c_int;

	/// Read data from an object
	///
	/// The io context determines the snapshot to read from, if any was set
	/// by rados_ioctx_snap_set_read().
	///
	/// @param io the context in which to perform the read
	/// @param oid the name of the object to read from
	/// @param buf where to store the results
	/// @param len the number of bytes to read
	/// @param off the offset to start reading from in the object
	/// @returns number of bytes read on success, negative error code on
	/// failure
 	fn rados_read(io: rados_ioctx_t, oid: *const c_char,
 		buf: *mut c_char, len: size_t, offset: u64) -> c_int;

	/// Get the value of an extended attribute on an object.
	///
	/// @param io the context in which the attribute is read
	/// @param o name of the object
	/// @param name which extended attribute to read
	/// @param buf where to store the result
	/// @param len size of buf in bytes
	/// @returns length of xattr value on success, negative error code on failure
	fn rados_getxattr(io: rados_ioctx_t, oid: *const c_char,
    	name: *const c_char, buf: *mut c_char, len: size_t) -> c_int;

	/// Set an extended attribute on an object.
	///
	/// @param io the context in which xattr is set
	/// @param o name of the object
	/// @param name which extended attribute to set
	/// @param buf what to store in the xattr
	/// @param len the number of bytes in buf
	/// @returns 0 on success, negative error code on failure
	fn rados_setxattr(io: rados_ioctx_t, oid: *const c_char,
		name: *const c_char, buf: *const c_char, len: size_t) -> c_int;

	/// Delete an object
	///
	/// @note This does not delete any snapshots of the object.
	///
	/// @param io the pool to delete the object from
	/// @param oid the name of the object to delete
	/// @returns 0 on success, negative error code on failure
  	fn rados_remove(io: rados_ioctx_t, oid: *const c_char) -> c_int;

	fn rados_ioctx_destroy(ioctx: c_void_ptr);

	fn rados_shutdown(cluster: c_void_ptr);
}

/// Get the version of librados.
///
/// The version number is `major.minor.extra`. Note that this is
/// unrelated to the Ceph version number.
///
/// # Examples
///
/// ```rust
/// println!("librados version: {}", ceph::rados::version());
/// ```
pub fn version() -> String {
	let mut major: c_int = -1;
	let mut minor: c_int = -1;
	let mut extra: c_int = -1;
	unsafe {
		rados_version(&mut major, &mut minor, &mut extra);
	}
	format!("{}.{}.{}", major, minor, extra)
}

pub struct Cluster {
	handle: rados_t
}

pub struct IoCtx {
	handle: rados_ioctx_t
}

pub trait StrStringOrNone {
	fn unwrap(self) -> Option<CString>;
}

impl StrStringOrNone for String {
	fn unwrap(self) -> Option<CString> {
		Some(CString::new(self).unwrap())
	}
}

impl StrStringOrNone for &'static str {
	fn unwrap(self) -> Option<CString> {
		Some(CString::new(self).unwrap())
	}
}

impl StrStringOrNone for Option<String> {
	fn unwrap(self) -> Option<CString> {
		self.map(|s| CString::new(s).unwrap())
	}
}

macro_rules! handle_errors {
	($x:expr) => {
		unsafe {
			let err = $x;
			if err < 0 {
				let s = CStr::from_ptr(strerror(err)).to_str().unwrap();
				println!("strerror({:?}) => {}", err, s);
				return Err(s);
			}
		}
	}
}

/// A neat way to allocate a zeroed out array of given size
///
/// # Examples
///
/// ```rust
/// let mut buf = zeroed_c_char_buf(100);
/// ```
macro_rules! zeroed_c_char_buf {
	($n:expr) => {
		repeat(0).take($n).collect::<Vec<c_char>>();
	}
}

impl Cluster {

	/// Create a handle for communicating with a RADOS cluster.
	///
	/// Ceph environment variables are read when this is called, so if
	/// $CEPH_ARGS specifies everything you need to connect, no further
	/// configuration is necessary.
	///
	/// # Parameters
	///
	/// * `cluster_name` the cluster name as a string, or `None` (equivalent to `null`)
	/// * `user_name` the full user name to connect as (i.e. "client.admin"`)
	/// * `flags` for future expansion
	///
	/// # Returns
	///
	/// * `Ok(Cluster)` on success
	/// * `Err(message: &str)` on failure
	pub fn create<'a, A, S>(cluster_name: A, user_name: S, flags: u64) -> Result<Cluster, &'a str>
		where A: StrStringOrNone,
		S: Into<Vec<u8>>
	{
	    let cluster_name_ptr = match cluster_name.unwrap() {
	    	None => ptr::null(),
	    	Some(cs) => cs.as_ptr()
	    };
	    let user_name_ptr = CString::new(user_name).unwrap().as_ptr();
		let handle: c_void_ptr = ptr::null_mut();
	    handle_errors!(rados_create2(&handle, cluster_name_ptr, user_name_ptr, flags));
		return Ok(Cluster { handle: handle });
	}


	/// Connect to the cluster.
	///
	/// # Note
	///
	/// **BUG:** Before calling this, calling a function that communicates with the
	/// cluster will crash.
	///
	/// # Prerequisites
	///
	/// The cluster handle is configured with at least a monitor address.
	/// If cephx is enabled, a client name and secret must also be set.
	///
	/// # Post
	///
	/// If this succeeds, any function in librados may be used
	///
	/// # Returns
	///
	/// * `Ok(())` on sucess
	/// * `Err(message: &str)` on failure
	pub fn connect(&self) -> Result<(), &str> {
		handle_errors!(rados_connect(self.handle));
		return Ok(());
	}

	/// Configure the cluster handle using a Ceph config file
	///
	/// If path is `NULL`, the default locations are searched, and the first
	/// found is used. The locations are:
	///
	/// * `$CEPH_CONF` (environment variable)
	/// * `/etc/ceph/ceph.conf`
	/// * `~/.ceph/config`
	/// * `ceph.conf` (in the current working directory)
	///
	/// # Prerequisites
	///
	/// `rados_connect()` has not been called on the cluster handle
	///
	/// # Parameters
	///
	/// * `cluster` cluster handle to configure
	/// * `path` path to a Ceph configuration file
	///
	/// # Returns
	///
	/// * `Ok(())` on success
	/// * `Err(message: &str)` on failure
	pub fn conf_read_file<S>(&self, config_filename: S) -> Result<(), &str>
		where S: StrStringOrNone
	{
	    let config_filename_ptr = match config_filename.unwrap() {
	    	None => ptr::null(),
	    	Some(cs) => cs.as_ptr()
	    };
		handle_errors!(rados_conf_read_file(self.handle, config_filename_ptr));
		return Ok(());
	}

	/// Configure the cluster handle with command line arguments
	///
	/// argv can contain any common Ceph command line option, including any
	/// configuration parameter prefixed by '--' and replacing spaces with
	/// dashes or underscores. For example, the following options are equivalent:
	///
	/// * `--mon-host 10.0.0.1:6789`
	/// * `--mon_host 10.0.0.1:6789`
	/// * `-m 10.0.0.1:6789`
	///
	/// # Prerequisites
	///
	/// `rados_connect()` has not been called on the cluster handle
	///
	/// # Parameters
	///
	/// * `cluster` cluster handle to configure
	/// * `args` Vec of String arguments, e.g. `env::args().collect()`
	///
	/// # Returns
	///
	/// * `Ok(())` on success
	/// * `Err(message: &str)` on failure
 	pub fn conf_parse_argv(&self, args: &Vec<String>) -> Result<(), &str> {
		let argc = args.len() as i32;
		let args_cs : Vec<CString> = args.iter().map(|a| CString::new(a.as_str()).unwrap()).collect();
		let argv : Vec<*const c_char> = args_cs.iter().map(|cs| cs.as_ptr()).collect();
		handle_errors!(rados_conf_parse_argv(self.handle, argc, argv.as_slice().as_ptr()));
		return Ok(());
	}

	/// Get the fsid of the cluster as a hexadecimal string.
	///
	/// The fsid is a unique id of an entire Ceph cluster.
	///
	/// # Returns
	///
	/// * `Ok(fsid: &str)` on success
	/// * `Err(message: &str)` on failure
	pub fn fsid(&self) -> Result<&str, &str> {
		// magic number
		let buf_size = 37;
		let mut buf = zeroed_c_char_buf!(buf_size);
		let buf_ptr = buf.as_mut_ptr();
		handle_errors!(rados_cluster_fsid(self.handle, buf_ptr as *mut c_char, buf_size as size_t));
 		return Ok(unsafe {
	 		CStr::from_ptr(buf_ptr).to_str().unwrap()
 		});
	}

	pub fn create_ioctx<S>(&self, pool_name: S) -> Result<IoCtx, &str>
		where S: Into<Vec<u8>>
	{
		let pool_name_ptr = CString::new(pool_name).unwrap().as_ptr();

		let ioctx_handle: c_void_ptr = ptr::null_mut();
		handle_errors!(rados_ioctx_create(self.handle, pool_name_ptr, &ioctx_handle));
		return Ok(IoCtx { handle: ioctx_handle });
	}

	pub fn shutdown(&self) {
		unsafe {
			rados_shutdown(self.handle);
		}
	}

}

impl Debug for Cluster {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		return f.write_fmt(format_args!("{:?}", self.handle))
	}
}

impl Drop for Cluster {
	fn drop(&mut self) {
		println!("rados_shutdown({:?})", self.handle);
		self.shutdown();
	}
}

#[allow(dead_code)]
/// Keep this here for debugging for now
fn dump(msg: &str, buf: *const c_char, len: isize) {
	print!("{}: ({}) [", msg, len);
	for i in 0..len {
		let c = unsafe {*buf.offset(i)};
		print!("{:02x}", c);
		if c >= 32 {
			print!(" '{}'", c as u8 as char);
		}
		if i < len - 1 {
			print!(",");
		}
	}
	println!("]");
}

impl IoCtx {
	pub fn write<S, T>(&self, oid: S, data: T) -> Result<(), &str>
		where S: Into<Vec<u8>>, T: Into<String>
	{
		let oid_cs = CString::new(oid).unwrap();
		let s : String = data.into();
		let len : size_t = s.len() as size_t;
		let buf = CString::new(s).unwrap();
		handle_errors!(rados_write_full(self.handle, oid_cs.as_ptr(), buf.as_ptr(), len));
		return Ok(());
	}

	pub fn read(&self, oid: &str, len: usize) -> Result<&str, &str> {
		// Need to hang on the the CString, can immediately do as_ptr()
		// see https://github.com/rust-lang/rust/issues/16035
		let oid_cs = CString::new(oid).unwrap();
		// allow for terminating '\0' (not really needed)
		let buf_size = len + 1;
		let mut buf = zeroed_c_char_buf!(buf_size);
		handle_errors!(rados_read(self.handle, oid_cs.as_ptr(), buf.as_mut_ptr(), buf_size as size_t, 0));
 		return Ok(unsafe {
	 		CStr::from_ptr(buf.as_ptr()).to_str().unwrap()
 		});
	}

	pub fn getxattr<S>(&self, oid: S, name: S, len: usize) -> Result<&str, &str>
		where S: Into<Vec<u8>>
	{
		// Need to hang on the the CString, can't immediately do as_ptr()
		// see https://github.com/rust-lang/rust/issues/16035
		let oid_cs = CString::new(oid).unwrap();
		let name_cs = CString::new(name).unwrap();
		// allow for terminating '\0' (not really needed)
		let buf_size = len + 1;
		// A neat way to allocate a zeroed out array of given size
		let mut buf = zeroed_c_char_buf!(buf_size);
		handle_errors!(rados_getxattr(self.handle, oid_cs.as_ptr(), name_cs.as_ptr(), buf.as_mut_ptr(), buf_size as size_t));
 		return Ok(unsafe {
	 		CStr::from_ptr(buf.as_ptr()).to_str().unwrap()
 		});
	}

	pub fn setxattr<S, T>(&self, oid: S, name: S, value: T) -> Result<(), &str>
		where S: Into<Vec<u8>>, T: Into<String>
	{
		// Need to hang on the the CString, can't immediately do as_ptr()
		// see https://github.com/rust-lang/rust/issues/16035
		let oid_cs = CString::new(oid).unwrap();
		let name_cs = CString::new(name).unwrap();
		// allow for terminating '\0' (not really needed)
		let s : String = value.into();
		let len : size_t = s.len() as size_t;
		let buf = CString::new(s).unwrap();
		handle_errors!(rados_setxattr(self.handle, oid_cs.as_ptr(), name_cs.as_ptr(), buf.as_ptr(), len));
		return Ok(());
	}

	pub fn remove(&self, oid: &str) -> Result<(), &str> {
		let oid_ptr = CString::new(oid).unwrap().as_ptr();
		handle_errors!(rados_remove(self.handle, oid_ptr));
		return Ok(());
	}
}


impl Drop for IoCtx {
	fn drop(&mut self) {
		println!("rados_ioctx_destroy({:?})", self.handle);
		unsafe {
			rados_ioctx_destroy(self.handle);
		}
	}
}
