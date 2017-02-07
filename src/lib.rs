extern crate conduit;
extern crate futures;
extern crate hyper;
extern crate semver;
extern crate unicase;

#[macro_use]
extern crate log;

use futures::Future;
use hyper::{HttpVersion, Method};
use hyper::header::{Headers as HyperHeaders, Host, ContentLength};
use hyper::server::{Request as HyperRequest, Response as HyperResponse};
use hyper::server::{Http, NewService, Service};
use hyper::status::StatusCode;
use std::collections::HashMap;
use std::io::{self, Read, Cursor};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;
use std::sync::{Arc, RwLock};
use unicase::UniCase;

struct Request {
    request: HyperRequest,
    scheme: conduit::Scheme,
    headers: Headers,
    extensions: conduit::Extensions,
}

fn ver(major: u64, minor: u64) -> semver::Version {
    semver::Version {
        major: major,
        minor: minor,
        patch: 0,
        pre: vec!(),
        build: vec!()
    }
}

impl conduit::Request for Request {
    fn http_version(&self) -> semver::Version {
        match *self.request.version() {
            HttpVersion::Http09 => ver(0, 9),
            HttpVersion::Http10 => ver(1, 0),
            HttpVersion::Http11 => ver(1, 1),
            HttpVersion::H2 => ver(2, 0),
            HttpVersion::H2c => ver(2,0),

            // NOTE: non-exhaustive patterns: `__DontMatchMe` not covered
            _ => unimplemented!(),
        }
    }

    fn conduit_version(&self) -> semver::Version {
        ver(0, 1)
    }

    fn method(&self) -> conduit::Method {
        match *self.request.method() {
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
        }
    }

    fn scheme(&self) -> conduit::Scheme {
        self.scheme
    }

    fn host<'c>(&'c self) -> conduit::Host<'c> {
        conduit::Host::Name(&self.request.headers().get::<Host>().unwrap().hostname())
    }

    fn virtual_root(&self) -> Option<&str> {
        None
    }

    fn path(&self) -> &str {
        self.request.path()
    }

    fn query_string(&self) -> Option<&str> {
        self.request.query()
    }

    fn remote_addr(&self) -> SocketAddr {
        *self.request.remote_addr()
    }

    fn content_length(&self) -> Option<u64> {
        self.request.headers().get::<ContentLength>().map(|h| h.0)
    }

    fn headers(&self) -> &conduit::Headers {
        &self.headers
    }

    fn body<'a>(&'a mut self) -> &'a mut Read { unimplemented!() }

    fn extensions(&self) -> &conduit::Extensions {
        &self.extensions
    }

    fn mut_extensions(&mut self) -> &mut conduit::Extensions {
        &mut self.extensions
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


// Server::http("0.0.0.0:3000")
//     .expect("could not start http server")
//     .handle(endpoint)
//     .expect("could not attach handler");

pub struct Server<H> {
    server: hyper::Server<ServiceAcceptor<H>>,
}

impl<H: conduit::Handler+'static> Server<H> {
    pub fn http(addr: SocketAddr, handler: H) -> hyper::error::Result<Server<H>> {
        let acceptor = ServiceAcceptor {
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

/// Accepts incoming requests and attaches them to the conduit handler
struct ServiceAcceptor<H> { 
    handler: Arc<H>,
    scheme: conduit::Scheme,
}

// NOTE: not sure why, but #[derive(Clone)] is returning &Self for
// this struct ... guess we'll do this ourselves.
impl<H> Clone for ServiceAcceptor<H> {
    fn clone(&self) -> Self {
        ServiceAcceptor {
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

    fn new_service(&self) -> Result<Self::Instance, io::Error> {
        Ok(self.clone())
    }
}

impl<H: conduit::Handler+'static> Service for ServiceAcceptor<H> {
    type Request  = HyperRequest;
    type Response = HyperResponse;
    type Error    = hyper::Error; // TODO: better error type?
    type Future   = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, request: Self::Request) -> Self::Future {
        let mut headers = HashMap::new();
        for header in request.headers().iter() {
            headers.entry(header.name().to_owned())
                .or_insert_with(Vec::new)
                .push(header.value_string());
        }

        let mut request = Request {
            request: request,
            scheme: self.scheme,
            headers: Headers(headers.into_iter().collect()),
            extensions: conduit::Extensions::new(),
        };

        let mut resp = match self.handler.call(&mut request) {
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

        futures::finished(response).boxed()
    }
}

fn respond(response: &mut Vec<u8>, body: &mut Box<conduit::WriteBody + Send>) -> io::Result<()> {
        body.write_body(response)?;
        Ok(())
}
