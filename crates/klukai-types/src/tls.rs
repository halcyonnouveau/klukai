use std::net::IpAddr;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, DnValue, IsCa,
    Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose, SanType,
};
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Rcgen(#[from] rcgen::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub fn generate_ca() -> Result<(Certificate, KeyPair), Error> {
    let key_pair = KeyPair::generate()?;
    let mut params = CertificateParams::default();

    params.key_identifier_method = KeyIdMethod::Sha384;

    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        DnValue::PrintableString("Corrosion Root CA".try_into()?),
    );
    params.distinguished_name = dn;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365 * 5);

    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let cert = params.self_signed(&key_pair)?;

    Ok((cert, key_pair))
}

pub fn generate_server_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    ip: IpAddr,
) -> Result<(Certificate, String, KeyPair), Error> {
    let ca_cert = ca_cert(ca_cert_pem, ca_key_pem)?;
    let key_pair = KeyPair::generate()?;

    let mut params = CertificateParams::default();

    params.key_identifier_method = KeyIdMethod::Sha384;

    let mut dn = DistinguishedName::new();
    dn.push(
        DnType::CommonName,
        DnValue::PrintableString("r.u.local".try_into()?),
    );
    params.distinguished_name = dn;

    params.subject_alt_names = vec![SanType::IpAddress(ip)];

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);

    let cert = params.signed_by(&key_pair, &ca_cert)?;
    let cert_signed = cert.pem();

    Ok((cert, cert_signed, key_pair))
}

fn ca_cert<'a>(
    ca_cert_pem: &'a str,
    ca_key_pem: &'a str,
) -> Result<Issuer<'a, KeyPair>, rcgen::Error> {
    let key_pair = KeyPair::from_pem(ca_key_pem)?;
    Issuer::from_ca_cert_pem(ca_cert_pem, key_pair)
}

pub fn generate_client_cert(
    ca_cert_pem: &str,
    ca_key_pem: &str,
) -> Result<(Certificate, String, KeyPair), Error> {
    let ca_cert = ca_cert(ca_cert_pem, ca_key_pem)?;
    let key_pair = KeyPair::generate()?;

    let mut params = CertificateParams::default();

    params.key_identifier_method = KeyIdMethod::Sha384;

    let dn = DistinguishedName::new();
    params.distinguished_name = dn;

    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);

    let cert = params.signed_by(&key_pair, &ca_cert)?;
    let cert_signed = cert.pem();

    Ok((cert, cert_signed, key_pair))
}
