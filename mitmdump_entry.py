import configparser
import sys

import passlib.handlers.argon2
import passlib.handlers.bcrypt
import passlib.handlers.cisco
import passlib.handlers.des_crypt
import passlib.handlers.digests
import passlib.handlers.django
import passlib.handlers.fshp
import passlib.handlers.ldap_digests
import passlib.handlers.md5_crypt
import passlib.handlers.misc
import passlib.handlers.mssql
import passlib.handlers.mysql
import passlib.handlers.oracle
import passlib.handlers.pbkdf2
import passlib.handlers.phpass
import passlib.handlers.postgres
import passlib.handlers.roundup
import passlib.handlers.scram
import passlib.handlers.scrypt
import passlib.handlers.sha1_crypt
import passlib.handlers.sha2_crypt
import passlib.handlers.sun_md5_crypt
import passlib.handlers.windows
from mitmproxy.tools.main import mitmdump


def main() -> None:
    raise SystemExit(mitmdump(args=sys.argv[1:]))


if __name__ == "__main__":
    main()
