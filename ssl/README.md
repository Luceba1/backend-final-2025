# SSL Certificates Directory

This directory is for SSL/TLS certificates when deploying with HTTPS support.

## Required Files

Place your SSL certificates here:
- `cert.pem` - SSL certificate file
- `key.pem` - Private key file
- `chain.pem` (optional) - Certificate chain file

## Security Notes

**IMPORTANT:**
- Never commit SSL certificates to version control
- Certificates in this directory are ignored by `.gitignore`
- Use strong private keys (minimum 2048-bit RSA or 256-bit ECDSA)
- Rotate certificates before expiration

## Generating Self-Signed Certificates (Development Only)

For development/testing only:

```bash
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout ssl/key.pem \
  -out ssl/cert.pem \
  -days 365 \
  -subj "/CN=localhost"
```

**WARNING:** Self-signed certificates should NEVER be used in production!

## Production Certificates

For production, use certificates from a trusted Certificate Authority (CA):

- **Let's Encrypt** (Free) - https://letsencrypt.org/
- **DigiCert**
- **GlobalSign**
- **Sectigo**

## Let's Encrypt with Certbot

To obtain free SSL certificates from Let's Encrypt:

```bash
# Install certbot
apt-get update
apt-get install certbot python3-certbot-nginx

# Obtain certificate
certbot --nginx -d yourdomain.com -d www.yourdomain.com

# Auto-renewal (certbot sets up cron job automatically)
certbot renew --dry-run
```

## File Permissions

Set secure permissions:

```bash
chmod 600 ssl/key.pem     # Private key - read-only by owner
chmod 644 ssl/cert.pem    # Certificate - readable by all
```

## Nginx Configuration

Once certificates are in place, uncomment the HTTPS server block in `nginx.conf` and update:

```nginx
ssl_certificate /etc/nginx/ssl/cert.pem;
ssl_certificate_key /etc/nginx/ssl/key.pem;
```

## Testing SSL Configuration

Use SSL Labs to test your SSL configuration:
https://www.ssllabs.com/ssltest/