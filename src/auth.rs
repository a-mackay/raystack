use base64;
use reqwest::{Client, Response};
use ring::{digest, hmac, pbkdf2};
use std::convert::From;
use std::num::NonZeroU32;
use std::str::FromStr;
use thiserror::Error;

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
            _ => Err(ParseHashFunctionError {
                unparsable_hash: s.to_owned(),
            }),
        }
    }
}

impl HashFunction {
    fn pbkdf2(
        &self,
        key: &[u8],
        salt: &[u8],
        iterations: NonZeroU32,
    ) -> Vec<u8> {
        let algorithm = match self {
            HashFunction::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            HashFunction::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        };
        let mut dk = vec![0u8; self.dk_len()];
        pbkdf2::derive(algorithm, iterations, salt, key, &mut dk);
        dk
    }

    /// Return the dk length, in bytes.
    fn dk_len(&self) -> usize {
        match self {
            HashFunction::Sha256 => 32,
            HashFunction::Sha512 => 64,
        }
    }

    /// See the documentation for hmac::Key::new for the restrictions on
    /// `key_value`.
    fn hmac_sign(&self, key_value: &[u8], data: &[u8]) -> hmac::Tag {
        let algorithm = match self {
            HashFunction::Sha256 => hmac::HMAC_SHA256,
            HashFunction::Sha512 => hmac::HMAC_SHA512,
        };
        let key = hmac::Key::new(algorithm, key_value);
        hmac::sign(&key, data)
    }

    fn digest(&self, input: &[u8]) -> Vec<u8> {
        let algorithm = match self {
            HashFunction::Sha256 => &digest::SHA256,
            HashFunction::Sha512 => &digest::SHA512,
        };

        let digest_result = digest::digest(algorithm, input);
        digest_result.as_ref().to_vec()
    }
}

type AuthResult<T> = std::result::Result<T, InternalAuthError>;

pub(crate) async fn new_auth_token(
    client: &Client,
    url: &str,
    username: &str,
    password: &str,
    rng: &ring::rand::SystemRandom,
) -> AuthResult<String> {
    let auth_session_cfg = auth_session_config(client, &url, username).await?;

    let AuthSessionConfig {
        handshake_token,
        hash_fn,
    } = auth_session_cfg;

    let nonce = generate_nonce(rng).map_err(HandshakeError::from)?;
    let client_first_msg = format!("n={},r={}", username, nonce);

    let server_first_res = server_first_response(
        client,
        &url,
        &handshake_token,
        &client_first_msg,
    )
    .await?;

    let ServerFirstResponse {
        server_first_msg,
        server_iterations,
        server_nonce,
        server_salt,
    } = server_first_res;

    let server_iterations = NonZeroU32::new(server_iterations)
        .expect("should never receive iterations = 0 from the server");

    let decoded_server_salt = base64::decode(&server_salt).map_err(|err| {
        HandshakeError::from(Base64DecodeError {
            msg: format!("{}", err),
        })
    })?;

    let salted_password = hash_fn.pbkdf2(
        password.as_bytes(),
        &decoded_server_salt,
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
    )
    .await?;

    let ServerSecondResponse {
        auth_token,
        server_signature,
    } = server_second_res;

    if is_server_valid(&auth_msg, &salted_password, &server_signature, &hash_fn)
    {
        Ok(auth_token)
    } else {
        Err(InternalAuthError::ServerValidation { server_id: url.into() })
    }
}

fn generate_nonce(
    rng: &dyn ring::rand::SecureRandom,
) -> Result<String, GenerateNonceError> {
    use std::fmt::Write;

    let mut out = vec![0u8; 32];
    rng.fill(&mut out).map_err(|err| GenerateNonceError {
        msg: format!("{}", err),
    })?;
    let mut nonce = String::new();
    for byte in out.iter() {
        write!(&mut nonce, "{:x}", byte).map_err(|err| GenerateNonceError {
            msg: format!("{}", err),
        })?;
    }
    Ok(nonce)
}

struct AuthSessionConfig {
    handshake_token: String,
    hash_fn: HashFunction,
}

async fn auth_session_config(
    client: &Client,
    url: &str,
    username: &str,
) -> AuthResult<AuthSessionConfig> {
    let base64_username = base64_encode_no_padding(username);
    let auth_header_value = format!("HELLO username={}", base64_username);
    let res = client
        .get(url)
        .header("Authorization", auth_header_value)
        .send()
        .await?;

    let kvps = parse_key_value_pairs_from_header("www-authenticate", res)?;
    let handshake_token = kvps.get("handshakeToken")?;
    let hash_fn = kvps
        .get("hash")?
        .parse::<HashFunction>()
        .map_err(HandshakeError::from)?;

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

async fn server_first_response(
    client: &Client,
    url: &str,
    handshake_token: &str,
    client_first_msg: &str,
) -> AuthResult<ServerFirstResponse> {
    let auth_header_value = format!(
        "SCRAM handshakeToken={}, data={}",
        handshake_token,
        base64_encode_no_padding(&client_first_msg)
    );
    let res = client
        .get(url)
        .header("Authorization", auth_header_value)
        .send()
        .await?;

    let kvps = parse_key_value_pairs_from_header("www-authenticate", res)?;
    let data_base64 = kvps.get("data")?;
    let data =
        base64_decode_no_padding(&data_base64).map_err(HandshakeError::from)?;

    let server_first_msg = data.clone();
    let data_kvps = parse_key_value_pairs(&data)?;
    let server_nonce = data_kvps.get("r")?;
    let server_salt = data_kvps.get("s")?;
    let server_iterations: u32 = data_kvps
        .get("i")?
        .parse()
        .map_err(|err| HandshakeError::from(ParseIterationsError::from(err)))?;

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

async fn server_second_response(
    client: &Client,
    url: &str,
    handshake_token: &str,
    auth_msg: &str,
    salted_password: &[u8],
    client_final_no_proof: &str,
    hash_fn: &HashFunction,
) -> AuthResult<ServerSecondResponse> {
    let client_key_tag = hash_fn.hmac_sign(&salted_password, b"Client Key");
    let client_key = client_key_tag.as_ref();
    let stored_key = hash_fn.digest(&client_key);
    let client_signature_tag =
        hash_fn.hmac_sign(&stored_key, auth_msg.as_bytes());
    let client_signature = client_signature_tag.as_ref();

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
        .send()
        .await?;

    let auth_info =
        parse_key_value_pairs_from_header("authentication-info", res)?;
    let auth_token = auth_info.get("authToken")?;
    let data_base64 = auth_info.get("data")?;
    let data =
        base64_decode_no_padding(&data_base64).map_err(HandshakeError::from)?;
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
    let computed_server_key_tag =
        hash_fn.hmac_sign(salted_password, b"Server Key");
    let computed_server_key = computed_server_key_tag.as_ref();
    let computed_server_signature_tag =
        hash_fn.hmac_sign(computed_server_key, auth_msg.as_bytes());
    let computed_server_signature = computed_server_signature_tag.as_ref();
    let computed_server_signature = base64::encode(computed_server_signature);

    computed_server_signature == server_signature
}

fn parse_key_value_pairs_from_header(
    header: &str,
    res: Response,
) -> Result<KeyValuePairs, KeyValuePairParseError> {
    let header_value = res.headers().get(header).ok_or_else(|| {
        let msg = format!("missing HTTP header {}", header);
        KeyValuePairParseError { msg }
    })?;
    let header_value_str = header_value.to_str().map_err(|_| {
        let msg =
            format!("could not convert HTTP header {} to a string", header);
        KeyValuePairParseError { msg }
    })?;
    parse_key_value_pairs(header_value_str)
}

fn parse_key_value_pairs(
    s: &str,
) -> Result<KeyValuePairs, KeyValuePairParseError> {
    let delimiters = &[' ', ','][..];

    let key_value_pairs: Result<Vec<_>, KeyValuePairParseError> = s
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
                let msg = format!("No '=' symbol in key-value pair {}", s);
                Err(KeyValuePairParseError { msg })
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
    fn get(&self, key: &str) -> Result<String, KeyValuePairParseError> {
        self.key_value_pairs
            .iter()
            .find(|(k, _v)| k == key)
            .map(|(_k, v)| v.clone())
            .ok_or_else(|| {
                let msg = format!("missing key {} in key-value pairs", key);
                KeyValuePairParseError { msg }
            })
    }
}

fn base64_encode_no_padding<T: ?Sized + AsRef<[u8]>>(s: &T) -> String {
    let config = base64::Config::new(base64::CharacterSet::Standard, false);
    base64::encode_config(s, config)
}

fn base64_decode_no_padding(s: &str) -> Result<String, Base64DecodeError> {
    let config = base64::Config::new(base64::CharacterSet::Standard, false);
    let bytes = base64::decode_config(s, config).map_err(|err| {
        let msg = format!("{}", err);
        Base64DecodeError { msg }
    })?;
    String::from_utf8(bytes).map_err(|err| {
        let msg = format!("{}", err);
        Base64DecodeError { msg }
    })
}

/// An error which occurred during the authentication process.
#[derive(Debug, Error)]
pub enum AuthError {
    /// An error which occurred in the underlying HTTP client.
    #[error("A HTTP client error occurred while authenticating: {0}")]
    Http(#[source] reqwest::Error),
    /// An error occurred in `raystack` during authentication.
    #[error("An internal error occurred while authenticating: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Error denoting that the server's identity was not valid.
    #[error("Could not validate the identity of the server {server_id}")]
    ServerValidation {
        server_id: String,
    },
}

impl From<InternalAuthError> for AuthError {
    fn from(err: InternalAuthError) -> Self {
        match err {
            InternalAuthError::Handshake(err) => {
                AuthError::Internal(Box::new(err))
            }
            InternalAuthError::Http(err) => AuthError::Http(err),
            InternalAuthError::ServerValidation {server_id } => AuthError::ServerValidation { server_id },
        }
    }
}

/// An error that occurred during the authentication process.
#[derive(Debug, Error)]
pub(crate) enum InternalAuthError {
    #[error("Error occured while authenticating with the server")]
    Handshake(#[from] HandshakeError),
    #[error("HTTP client error")]
    Http(#[from] reqwest::Error),
    #[error("Could not validate the identity of the server {server_id}")]
    ServerValidation {
        server_id: String,
    },
}

/// An error that occurred during the authentication handshake.
#[derive(Debug, Error)]
pub(crate) enum HandshakeError {
    #[error("{0}")]
    Base64Decode(#[from] Base64DecodeError),
    #[error("{0}")]
    GenerateNonce(#[from] GenerateNonceError),
    #[error("Could not convert a HTTP header to a string")]
    HeaderToStr(#[from] reqwest::header::ToStrError),
    #[error("{0}")]
    KeyValuePairParse(#[from] KeyValuePairParseError),
    #[error("{0}")]
    ParseHashFunction(#[from] ParseHashFunctionError),
    #[error("{0}")]
    ParseIterations(#[from] ParseIterationsError),
    #[error("Could not decode a string as UTF8")]
    Utf8Decode(#[from] std::string::FromUtf8Error),
}

#[derive(Debug, Error)]
#[error("Unsupported hash function")]
pub(crate) struct ParseHashFunctionError {
    unparsable_hash: String,
}

#[derive(Debug, Error)]
#[error("Could not parse the iteration count as an integer")]
pub(crate) struct ParseIterationsError(#[from] std::num::ParseIntError);

#[derive(Debug, Error)]
#[error("Could not parse key-value pair: {msg}")]
pub(crate) struct KeyValuePairParseError {
    msg: String,
}

impl KeyValuePairParseError {
    fn into_auth_error(self) -> InternalAuthError {
        InternalAuthError::from(HandshakeError::from(self))
    }
}

impl From<KeyValuePairParseError> for InternalAuthError {
    fn from(err: KeyValuePairParseError) -> Self {
        err.into_auth_error()
    }
}

#[derive(Debug, Error)]
#[error("Could not decode a base64-encoded string, cause: {msg}")]
pub(crate) struct Base64DecodeError {
    msg: String,
}

#[derive(Debug, Error)]
#[error("Could not generate a nonce, cause: {msg}")]
pub(crate) struct GenerateNonceError {
    msg: String,
}
