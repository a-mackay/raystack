use base64;
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::pbkdf2::pbkdf2;
use crypto::sha2::{Sha256, Sha512};
use rand::random;
use reqwest::{Client, Response};
use std::str::FromStr;
use std::{convert::From, error, fmt};

#[derive(Debug)]
struct ParseHashFunctionError;

enum HashFunction {
    Sha256,
    Sha512,
}

impl FromStr for HashFunction {
    type Err = ParseHashFunctionError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "SHA-256" => Ok(HashFunction::Sha256),
            "SHA-512" => Ok(HashFunction::Sha512),
            _ => Err(ParseHashFunctionError),
        }
    }
}

impl HashFunction {
    fn pbkdf2(&self, key: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut dk = vec![0u8; self.dk_len()];

        match self {
            HashFunction::Sha256 => {
                let mut hmac = Hmac::new(Sha256::new(), key);
                pbkdf2(&mut hmac, salt, iterations, &mut dk);
            }
            HashFunction::Sha512 => {
                let mut hmac = Hmac::new(Sha512::new(), key);
                pbkdf2(&mut hmac, salt, iterations, &mut dk);
            }
        };
        dk
    }

    /// Return the dk length, in bytes.
    fn dk_len(&self) -> usize {
        match self {
            HashFunction::Sha256 => 32,
            HashFunction::Sha512 => 64,
        }
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let mac_result = match self {
            HashFunction::Sha256 => {
                let mut hmac = Hmac::new(Sha256::new(), key);
                hmac.input(data);
                hmac.result()
            }
            HashFunction::Sha512 => {
                let mut hmac = Hmac::new(Sha512::new(), key);
                hmac.input(data);
                hmac.result()
            }
        };

        // WARNING: Calls to code() may be vulnerable to timing attacks,
        // see the documentation for that function for more details.
        mac_result.code().into()
    }

    fn digest(&self, input: &[u8]) -> Vec<u8> {
        match self {
            HashFunction::Sha256 => {
                let mut sha = Sha256::new();
                sha.input(input);
                let mut digest = vec![0u8; sha.output_bytes()];
                sha.result(&mut digest);
                digest
            }
            HashFunction::Sha512 => {
                let mut sha = Sha512::new();
                sha.input(input);
                let mut digest = vec![0u8; sha.output_bytes()];
                sha.result(&mut digest);
                digest
            }
        }
    }
}

type Result<T> = std::result::Result<T, AuthError>;

pub(crate) fn new_auth_token(
    client: &Client,
    url: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let auth_session_cfg = auth_session_config(client, &url, username);

    let AuthSessionConfig {
        handshake_token,
        hash_fn,
    } = auth_session_cfg?;

    let nonce = format!("{:x}", random::<i128>());
    let client_first_msg = format!("n={},r={}", username, nonce);

    let server_first_res = server_first_response(
        client,
        &url,
        &handshake_token,
        &client_first_msg,
    );

    let ServerFirstResponse {
        server_first_msg,
        server_iterations,
        server_nonce,
        server_salt,
    } = server_first_res?;

    let salted_password = hash_fn.pbkdf2(
        password.as_bytes(),
        &base64::decode(&server_salt)?,
        server_iterations,
    );

    let client_final_no_proof = format!("c=biws,r={}", server_nonce);
    let auth_msg = format!(
        "{},{},{}",
        client_first_msg, server_first_msg, client_final_no_proof
    );

    let server_second_res = server_second_response(
        client,
        &url,
        &handshake_token,
        &auth_msg,
        &salted_password,
        &client_final_no_proof,
        &hash_fn,
    );

    let ServerSecondResponse {
        auth_token,
        server_signature,
    } = server_second_res?;

    if is_server_valid(&auth_msg, &salted_password, &server_signature, &hash_fn)
    {
        Ok(auth_token)
    } else {
        Err(AuthError {
            kind: AuthErrorKind::ServerValidationError,
        })
    }
}

struct AuthSessionConfig {
    handshake_token: String,
    hash_fn: HashFunction,
}

fn auth_session_config(
    client: &Client,
    url: &str,
    username: &str,
) -> std::result::Result<AuthSessionConfig, AuthError> {
    let base64_username = base64_encode_no_padding(username);
    let auth_header_value = format!("HELLO username={}", base64_username);
    let res = client
        .get(url)
        .header("Authorization", auth_header_value)
        .send()?;

    let kvps = parse_key_value_pairs_from_header("www-authenticate", res)?;
    let handshake_token = kvps.get("handshakeToken")?;
    let hash_fn = kvps.get("hash")?.parse::<HashFunction>()?;

    Ok(AuthSessionConfig {
        handshake_token,
        hash_fn,
    })
}

struct ServerFirstResponse {
    server_first_msg: String,
    server_iterations: u32,
    server_nonce: String,
    server_salt: String,
}

fn server_first_response(
    client: &Client,
    url: &str,
    handshake_token: &str,
    client_first_msg: &str,
) -> Result<ServerFirstResponse> {
    let auth_header_value = format!(
        "SCRAM handshakeToken={}, data={}",
        handshake_token,
        base64_encode_no_padding(&client_first_msg)
    );
    let res = client
        .get(url)
        .header("Authorization", auth_header_value)
        .send()?;

    let kvps = parse_key_value_pairs_from_header("www-authenticate", res)?;
    let data_base64 = kvps.get("data")?;
    let data = base64_decode_no_padding(&data_base64)?;

    let server_first_msg = data.clone();
    let data_kvps = parse_key_value_pairs(&data)?;
    let server_nonce = data_kvps.get("r")?;
    let server_salt = data_kvps.get("s")?;
    let server_iterations: u32 = data_kvps.get("i")?.parse()?;

    Ok(ServerFirstResponse {
        server_first_msg,
        server_iterations,
        server_nonce,
        server_salt,
    })
}

struct ServerSecondResponse {
    auth_token: String,
    server_signature: String,
}

fn server_second_response(
    client: &Client,
    url: &str,
    handshake_token: &str,
    auth_msg: &str,
    salted_password: &[u8],
    client_final_no_proof: &str,
    hash_fn: &HashFunction,
) -> Result<ServerSecondResponse> {
    let client_key = hash_fn.hmac(&salted_password, b"Client Key");
    let stored_key = hash_fn.digest(&client_key);
    let client_signature = hash_fn.hmac(&stored_key, auth_msg.as_bytes());

    let client_proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature)
        .map(|(key_byte, sig_byte)| key_byte ^ sig_byte)
        .collect();

    let client_final = format!(
        "{},p={}",
        client_final_no_proof,
        base64_encode_no_padding(&client_proof)
    );

    let auth_header_value = format!(
        "SCRAM handshakeToken={}, data={}",
        handshake_token,
        base64_encode_no_padding(&client_final)
    );
    let res = client
        .get(url)
        .header("Authorization", auth_header_value)
        .send()?;

    let auth_info =
        parse_key_value_pairs_from_header("authentication-info", res)?;
    let auth_token = auth_info.get("authToken")?;
    let data_base64 = auth_info.get("data")?;
    let data = base64_decode_no_padding(&data_base64)?;
    let server_signature = parse_key_value_pairs(&data)?.get("v")?;

    Ok(ServerSecondResponse {
        auth_token,
        server_signature,
    })
}

fn is_server_valid(
    auth_msg: &str,
    salted_password: &[u8],
    server_signature: &str,
    hash_fn: &HashFunction,
) -> bool {
    let computed_server_key = hash_fn.hmac(salted_password, b"Server Key");
    let computed_server_signature =
        hash_fn.hmac(&computed_server_key, auth_msg.as_bytes());
    let computed_server_signature = base64::encode(&computed_server_signature);

    computed_server_signature == server_signature
}

fn parse_key_value_pairs_from_header(
    header: &str,
    res: Response,
) -> Result<KeyValuePairs> {
    let header_value = res
        .headers()
        .get(header)
        .ok_or_else(|| AuthError::new_missing_response_data(header))?;
    let header_value_str = header_value.to_str()?;
    parse_key_value_pairs(header_value_str)
}

fn parse_key_value_pairs(s: &str) -> Result<KeyValuePairs> {
    let delimiters = &[' ', ','][..];

    let key_value_pairs: Result<Vec<_>> = s
        .split(delimiters)
        .filter(|s| s.to_lowercase() != "scram" && !s.is_empty())
        .map(|s| {
            let delimiter_index = s.find('=');

            if let Some(delimiter_index) = delimiter_index {
                let split = s.split_at(delimiter_index);
                let key = split.0.to_string();
                let value = split.1.trim_start_matches('=').to_string();
                Ok((key, value))
            } else {
                let description =
                    format!("No '=' symbol in key-value pair {}", s);
                Err(AuthError {
                    kind: AuthErrorKind::ParseError { description },
                })
            }
        })
        .collect();

    Ok(KeyValuePairs {
        key_value_pairs: key_value_pairs?,
    })
}

struct KeyValuePairs {
    key_value_pairs: Vec<(String, String)>,
}

impl KeyValuePairs {
    fn get(&self, key: &str) -> Result<String> {
        self.key_value_pairs
            .iter()
            .find(|(k, _v)| k == key)
            .map(|(_k, v)| v.clone())
            .ok_or_else(|| AuthError::new_missing_response_data(key))
    }
}

fn base64_encode_no_padding<T: ?Sized + AsRef<[u8]>>(s: &T) -> String {
    let config = base64::Config::new(base64::CharacterSet::Standard, false);
    base64::encode_config(s, config)
}

fn base64_decode_no_padding(s: &str) -> Result<String> {
    let config = base64::Config::new(base64::CharacterSet::Standard, false);
    let bytes = base64::decode_config(s, config)?;
    String::from_utf8(bytes).map_err(|e| e.into())
}

/// An error that occurred during the authentication process.
#[derive(Debug)]
pub struct AuthError {
    kind: AuthErrorKind,
}

impl AuthError {
    pub(crate) fn kind(&self) -> &AuthErrorKind {
        &self.kind
    }

    fn new_missing_response_data(data_description: &str) -> Self {
        AuthError {
            kind: AuthErrorKind::MissingResponseData {
                data_description: data_description.to_owned(),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) enum AuthErrorKind {
    Base64DecodeError,
    HeaderToStrError,
    HttpError { err: reqwest::Error },
    MissingResponseData { data_description: String },
    ParseError { description: String },
    ServerValidationError,
    Utf8DecodeError(std::string::FromUtf8Error),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self.kind() {
            AuthErrorKind::Base64DecodeError => {
                "Could not decode base64".to_owned()
            }
            AuthErrorKind::HeaderToStrError => {
                "Could not convert header to a string".to_owned()
            }
            AuthErrorKind::MissingResponseData { data_description } => format!(
                "Response from server is missing some expected information: {}",
                data_description
            ),
            AuthErrorKind::ParseError { description } => {
                format!("Parsing error: {}", description)
            }
            AuthErrorKind::HttpError { err } => {
                format!("HTTP library error: {}", err)
            }
            AuthErrorKind::ServerValidationError => {
                "Could not validate the identity of the server".to_owned()
            }
            AuthErrorKind::Utf8DecodeError(_) => {
                "Could not decode UTF8".to_owned()
            }
        };
        write!(f, "Authorization error: {}", msg)
    }
}

impl From<base64::DecodeError> for AuthError {
    fn from(_: base64::DecodeError) -> Self {
        AuthError {
            kind: AuthErrorKind::Base64DecodeError,
        }
    }
}

impl From<std::string::FromUtf8Error> for AuthError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        AuthError {
            kind: AuthErrorKind::Utf8DecodeError(error),
        }
    }
}

impl From<reqwest::header::ToStrError> for AuthError {
    fn from(_: reqwest::header::ToStrError) -> Self {
        AuthError {
            kind: AuthErrorKind::HeaderToStrError,
        }
    }
}

impl From<ParseHashFunctionError> for AuthError {
    fn from(_: ParseHashFunctionError) -> Self {
        let description = "Unknown hash function".to_owned();
        AuthError {
            kind: AuthErrorKind::ParseError { description },
        }
    }
}

impl From<std::num::ParseIntError> for AuthError {
    fn from(_: std::num::ParseIntError) -> Self {
        let description = "Could not parse integer".to_owned();
        AuthError {
            kind: AuthErrorKind::ParseError { description },
        }
    }
}

impl From<reqwest::Error> for AuthError {
    fn from(error: reqwest::Error) -> Self {
        AuthError {
            kind: AuthErrorKind::HttpError { err: error },
        }
    }
}

impl error::Error for AuthError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self.kind() {
            AuthErrorKind::Base64DecodeError => None,
            AuthErrorKind::HeaderToStrError => None,
            AuthErrorKind::HttpError { err } => Some(err),
            AuthErrorKind::MissingResponseData { .. } => None,
            AuthErrorKind::ParseError { .. } => None,
            AuthErrorKind::ServerValidationError => None,
            AuthErrorKind::Utf8DecodeError(err) => Some(err),
        }
    }
}
