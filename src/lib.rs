extern crate conduit;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate semver;
extern crate unicase;

#[macro_use]
extern crate log;

use futures::{future, Async, Future, Poll, Stream};
use futures_cpupool::CpuPool;
use hyper::{Body, HttpVersion, Method};
use hyper::header::{Headers as HyperHeaders, Host, ContentLength};
use hyper::server::{Request as HyperRequest, Response as HyperResponse};
use hyper::server::{Http, NewService, Service};
use hyper::status::StatusCode;
use std::collections::HashMap;
use std::io::{self, Cursor, Read};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;
use std::sync::Arc;
use unicase::UniCase;

/// This is a request body which has not yet been read in it's entirety.
///
/// Since conduit expects an `io::Read` impl from the stdlib's synchronous IO
/// we buffer the response body until it is ready to be read in its entirety.
struct RequestBuffer {
    is_stolen: bool,
    request_buf:  Option<Vec<u8>>,
    request_body: Body,
}

impl RequestBuffer {
    fn from(body: Body) -> Self {
        RequestBuffer {
            is_stolen: false,
            request_buf: Some(vec![]),
            request_body: body,
        }
    }

    fn append_chunk(&mut self, chunk: &[u8])  {
        self.request_buf.as_mut().unwrap().extend_from_slice(chunk);
    }
}

impl Future for RequestBuffer {
    type Item  = Cursor<Vec<u8>>;
    type Error = io::Error;

    /// Polling this future means reading the next chunk from our request body
    /// until we've buffered it entirely...
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.request_body.poll() {
            // handle a chunk from the request stream
            Ok(Async::Ready(Some(body_chunk))) => {
                self.append_chunk(&*body_chunk);
                Ok(Async::NotReady)
            },

            Ok(Async::Ready(None)) => {
                // TODO: it is unsafe to call #poll() again after we do this ...
                assert_eq!(self.is_stolen, false); self.is_stolen = true;

                let finished_buffer = self.request_buf.take().unwrap();
                Ok(Async::Ready(Cursor::new(finished_buffer)))
            },

            Ok(Async::NotReady) => Ok(Async::NotReady),


            _ => unimplemented!(),
        }
    }
}

/// This represents info which conduit will need from the incoming
/// HTTP request. This is split out from the extension storage so that it
/// can be safely moved between threads while we await the response body.
///
/// This data must be parsed from a request before the body can be read,
/// since taking the `Body` stream from a `hyper::Request` will consume
/// said request.
struct RequestInfo {
    http_version: semver::Version,
    conduit_version: semver::Version,
    method: conduit::Method,
    scheme: conduit::Scheme,
    host_name: String,
    path: String,
    query_string: Option<String>,
    remote_addr: SocketAddr,
    content_length: Option<u64>,
    headers: Headers,
}

impl RequestInfo {
    fn new(request: &HyperRequest, 
           scheme: conduit::Scheme, 
           headers: Headers) -> RequestInfo {

         let version = match *request.version() {
            HttpVersion::Http09 => ver(0, 9),
            HttpVersion::Http10 => ver(1, 0),
            HttpVersion::Http11 => ver(1, 1),
            HttpVersion::H2 => ver(2, 0),
            HttpVersion::H2c => ver(2,0),

            // NOTE: non-exhaustive patterns: `__DontMatchMe` not covered
            _ => unimplemented!(),
        };

        let method = match *request.method() {
            Method::Connect => conduit::Method::Connect,
            Method::Delete => conduit::Method::Delete,
            Method::Get => conduit::Method::Get,
            Method::Head => conduit::Method::Head,
            Method::Options => conduit::Method::Options,
            Method::Patch => conduit::Method::Patch,
            Method::Post => conduit::Method::Post,
            Method::Put => conduit::Method::Put,
            Method::Trace => conduit::Method::Trace,
            // https://github.com/conduit-rust/conduit/pull/12
            Method::Extension(_) => unimplemented!(),
        };

        let host = request.headers()
            .get::<Host>()
            .unwrap()
            .hostname()
            .to_string();

        let path = request.path().to_string();
        let query_string = request.query().map(|query| query.to_owned());
        let remote_addr = *request.remote_addr();
        let content_length = request.headers().get::<ContentLength>().map(|h| h.0);

        RequestInfo {
            http_version: version,
            conduit_version: ver(0,1),
            method: method,
            scheme: scheme,
            host_name: host,
            path: path,
            query_string: query_string,
            remote_addr: remote_addr,
            content_length: content_length,
            headers: headers,
        }
    }
}

/// This represents a completely loaded request, which is ready
/// to be scheduled to a CPU core and handled by the conduit application.
struct CompletedRequest {
    req_body: Cursor<Vec<u8>>,
    req_info: RequestInfo,
    extensions: conduit::Extensions,
}

impl CompletedRequest {
    fn from(req_info: RequestInfo, req_body: Cursor<Vec<u8>>) -> Self {
        CompletedRequest {
            req_body: req_body,
            req_info: req_info,
            extensions: conduit::Extensions::new(),
        }
    }
}

impl conduit::Request for CompletedRequest {
    fn http_version(&self) -> semver::Version { self.req_info.http_version.clone() }
    fn conduit_version(&self) -> semver::Version { self.req_info.conduit_version.clone() }
    fn method(&self) -> conduit::Method { self.req_info.method.clone() }
    fn scheme(&self) -> conduit::Scheme { self.req_info.scheme }
    fn headers(&self) -> &conduit::Headers { &self.req_info.headers }
    fn content_length(&self) -> Option<u64> { self.req_info.content_length }
    fn remote_addr(&self) -> SocketAddr { self.req_info.remote_addr }
    fn virtual_root(&self) -> Option<&str> { None }
    fn path(&self) -> &str { &self.req_info.path }
    fn extensions(&self) -> &conduit::Extensions { &self.extensions }
    fn mut_extensions(&mut self) -> &mut conduit::Extensions { &mut self.extensions }


    fn host<'c>(&'c self) -> conduit::Host<'c> {
        conduit::Host::Name(&self.req_info.host_name)
    }

    fn query_string(&self) -> Option<&str> {
        self.req_info.query_string.as_ref().map(|inner| &**inner)
    }

    fn body<'a>(&'a mut self) -> &'a mut Read { 
        self.req_body.set_position(0); &mut self.req_body
    }
}

struct Headers(Vec<(String, Vec<String>)>);

impl Headers {
    fn find_raw(&self, key: &str) -> Option<&[String]> {
        self.0.iter().find(|&&(ref k, _)| UniCase(k) == key).map(|&(_, ref vs)| &**vs)
    }
}

impl conduit::Headers for Headers {
    fn find(&self, key: &str) -> Option<Vec<&str>> {
        self.find_raw(key).map(|vs| vs.iter().map(|v| &**v).collect())
    }

    fn has(&self, key: &str) -> bool {
        self.find_raw(key).is_some()
    }

    fn all(&self) -> Vec<(&str, Vec<&str>)> {
        self.0.iter().map(|&(ref k, ref vs)| (&**k, vs.iter().map(|v| &**v).collect())).collect()
    }
}

pub struct Server<H> {
    server: hyper::Server<ServiceAcceptor<H>>,
}

impl<H: conduit::Handler+'static> Server<H> {
    pub fn http(addr: SocketAddr, handler: H) -> hyper::error::Result<Server<H>> {
        let acceptor = ServiceAcceptor {
            cpu_pool: CpuPool::new_num_cpus(),
            handler: Arc::new(handler),
            scheme: conduit::Scheme::Http,
        };

        Ok(Server {
            server: Http::new().bind(&addr, acceptor)?,
        })
    }

    pub fn run(self) -> hyper::error::Result<()> {
        self.server.run()
    }
}

/// This structure accepts incoming HTTP requests and maintains enough
/// state to cooperatively schedule them on the event loop.
///
/// It is implemented as a `conduit::Handler` which runs arbitrary application
/// code, along with a CPU pool to prevent said application from blocking the
/// evented I/O.
struct ServiceAcceptor<H> { 
    cpu_pool: CpuPool,
    handler: Arc<H>,
    scheme: conduit::Scheme,
}

// NOTE: not sure why, but #[derive(Clone)] is returning &Self for
// this struct ... guess we'll do this ourselves.
impl<H> Clone for ServiceAcceptor<H> {
    fn clone(&self) -> Self {
        ServiceAcceptor {
            cpu_pool: self.cpu_pool.clone(),
            handler: self.handler.clone(),
            scheme:  self.scheme,
        }
    }
}

impl<H: conduit::Handler> NewService for ServiceAcceptor<H> {
    type Request  = HyperRequest;
    type Response = HyperResponse;
    type Error    = hyper::Error; // TODO: better error type?
    type Instance = ServiceAcceptor<H>;

    /// This service furnishes requests by simply cloning itself.
    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(self.clone())
    }
}

impl<H: conduit::Handler+'static> Service for ServiceAcceptor<H> {
    type Request  = HyperRequest;
    type Response = HyperResponse;
    type Error    = hyper::Error; // TODO: better error type?
    type Future   = Box<Future<Item = Self::Response, Error = Self::Error>>;

    /// Incoming requests are handled in a few distinct phases:
    /// 
    /// - First we parse the incoming request information and parse
    ///   it into a format which `conduit` is expecting. We set this
    ///   request info aside.
    ///   
    /// - Next we buffer the response body until it has either been
    ///   read in its entirety, or the remote peer hung up.
    ///
    /// - Lastly we combine the buffer & request info into a single
    ///   `conduit::Request` trait object which is passed to the application
    ///   layer to ultimately generate a response. (To prevent stalling the
    ///   event loop we schedule this application code to run on a thread pool.)
    /// 
    /// - Finally the response is emitted and `hyper` will take care of the rest.
    ///
    fn call(&self, request: Self::Request) -> Self::Future {
        let mut headers = HashMap::new();
        for header in request.headers().iter() {
            headers.entry(header.name().to_owned())
                .or_insert_with(Vec::new)
                .push(header.value_string());
        }

        let app_request = RequestInfo::new(
            &request,
            self.scheme,
            Headers(headers.into_iter().collect()),
        );

        // NOTE: we explicitly clone these out here, as passing `self` into the
        //       closures below tends to produce an error that the future does
        //       not outlive the 'static lifetime.
        //
        let thread_worker = self.cpu_pool.clone();
        let thread_handler = self.handler.clone();
        Box::new(RequestBuffer::from(request.body()).and_then(move |req_body| {
            thread_worker.spawn_fn(move || {
                let mut app_request = CompletedRequest::from(app_request, req_body);
                let mut resp = match thread_handler.call(&mut app_request) {
                    Ok(response) => response,
                    Err(e) => {
                        error!("Unhandled error: {}", e);
                        conduit::Response {
                            status: (500, "Internal Server Error"),
                            headers: HashMap::new(),
                            body: Box::new(Cursor::new(e.to_string().into_bytes())),
                        }
                    }
                };

                let mut outgoing_headers = HyperHeaders::with_capacity(resp.headers.len());
                for (key, value) in resp.headers {
                    let value: Vec<_> = value.into_iter().map(|s| s.into_bytes()).collect();
                    outgoing_headers.set_raw(key, value);
                }
     
                // TODO: take advantage of tokio to stream resp here?
                let mut buf = vec![];
                if let Err(e) = respond(&mut buf, &mut resp.body) {
                    error!("Error sending response: {:?}", e);    
                };
     
                let response = HyperResponse::new()
                    .with_status(StatusCode::from_u16(resp.status.0 as u16))
                    .with_headers(outgoing_headers)
                    .with_body(buf);
     
                future::ok(response).boxed()
            }).boxed()
        }).map_err(|io_err| {
            hyper::Error::Io(io_err) 
        }))
    }
}

/// Helper to create a version identifier
fn ver(major: u64, minor: u64) -> semver::Version {
    semver::Version {
        major: major,
        minor: minor,
        patch: 0,
        pre: vec!(),
        build: vec!()
    }
}

/// Helper to copy the `conduit` response body into a byte buffer
fn respond(response: &mut Vec<u8>, body: &mut Box<conduit::WriteBody + Send>) -> io::Result<()> {
        body.write_body(response)?;
        Ok(())
}
