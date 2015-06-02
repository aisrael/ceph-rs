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
type rados_ioctx_t = c_void_ptr;

#[link(name = "rados")]
#[allow(dead_code)]
extern "C" {
	fn rados_version(major: *mut c_int, minor: *mut c_int, extra: *mut c_int);
	fn rados_create(cluster: &c_void_ptr, id: *const c_char) -> c_int;
	fn rados_create2(cluster: &c_void_ptr, cluster_name: *const c_char,
		user_name: *const c_char, flags: u64) -> c_int;
	fn rados_conf_read_file(cluster: c_void_ptr, path: *const c_char) -> c_int;
	fn rados_conf_parse_argv(cluster: c_void_ptr, argc: c_int, argv: *const *const c_char) -> c_int;
	fn rados_connect(cluster: c_void_ptr) -> c_int;

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
 	fn rados_read(io: rados_ioctx_t, oid: *const c_char, buf: *mut c_char, len: size_t, offset: u64) -> c_int;

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
	handle: c_void_ptr
}

pub struct IoCtx {
	handle: c_void_ptr
}

pub trait ClusterNameArg {
	fn unwrap(self) -> Option<CString>;
}

impl ClusterNameArg for String {
	fn unwrap(self) -> Option<CString> {
		Some(CString::new(self).unwrap())
	}
}

impl ClusterNameArg for &'static str {
	fn unwrap(self) -> Option<CString> {
		Some(CString::new(self).unwrap())
	}
}

impl ClusterNameArg for Option<String> {
	fn unwrap(self) -> Option<CString> {
		match self {
			None => None,
			Some(s) => Some(CString::new(s).unwrap())
		}
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

impl Cluster {
	pub fn create<'lifetime, A: ClusterNameArg, S: Into<Vec<u8>>>(cluster_name: A, user_name: S, flags: u64) -> Result<Cluster, &'lifetime str> {
	    let cluster_name_ptr = match cluster_name.unwrap() {
	    	None => ptr::null(),
	    	Some(cs) => cs.as_ptr()
	    };
	    let user_name_ptr = CString::new(user_name).unwrap().as_ptr();
		let handle: c_void_ptr = ptr::null_mut();
	    handle_errors!(rados_create2(&handle, cluster_name_ptr, user_name_ptr, flags));
		return Ok(Cluster { handle: handle });
	}

	pub fn conf_read_file<S: Into<Vec<u8>>>(&self, config_filename: S) -> Result<(), &str> {
		let path = CString::new(config_filename).unwrap();
		handle_errors!(rados_conf_read_file(self.handle, path.as_ptr()));
		return Ok(());
	}

	pub fn conf_parse_argv(&self, args: &Vec<String>) -> Result<(), &str> {
		let argc = args.len() as i32;
		let args_cs : Vec<CString> = args.iter().map(|a| CString::new(a.as_str()).unwrap()).collect();
		let argv : Vec<*const c_char> = args_cs.iter().map(|cs| cs.as_ptr()).collect();
		handle_errors!(rados_conf_parse_argv(self.handle, argc, argv.as_slice().as_ptr()));
		return Ok(());
	}

	pub fn connect(&self) -> Result<(), &str> {
		handle_errors!(rados_connect(self.handle));
		return Ok(());
	}

	pub fn create_ioctx<S: Into<Vec<u8>>>(&self, pool_name: S) -> Result<IoCtx, &str> {
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
	pub fn write<S: Into<Vec<u8>>, T: Into<String>>(&self, oid: S, data: T) -> Result<(), &str> {
		let oid_cs = CString::new(oid).unwrap();
		let s : String = data.into();
		let buf = CString::new(s).unwrap();
		let buf_ptr = buf.as_ptr();
		let len : size_t = buf.as_bytes().len() as size_t;
		handle_errors!(rados_write_full(self.handle, oid_cs.as_ptr(), buf_ptr, len));
		return Ok(());
	}

	pub fn read(&self, oid: &str, len: usize) -> Result<&str, &str> {
		use std::iter::repeat;

		// Need to hang on the the CString, can immediately do as_ptr()
		// see https://github.com/rust-lang/rust/issues/16035
		let oid_cs = CString::new(oid).unwrap();
		// allow for terminating '\0' (not really needed)
		let buf_size = len + 1;
		// A neat way to allocate a zeroed out array of given size
		let mut buf = repeat(0).take(buf_size).collect::<Vec<c_char>>();
		let buf_ptr = buf.as_mut_ptr();
		handle_errors!(rados_read(self.handle, oid_cs.as_ptr(), buf_ptr as *mut c_char, buf_size as size_t, 0));
 		return Ok(unsafe {
	 		CStr::from_ptr(buf_ptr).to_str().unwrap()
 		});
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
