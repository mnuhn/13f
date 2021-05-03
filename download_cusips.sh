#!/bin/sh

# Downloads a list of "CUSIP -> COMPANY NAME" mappings.
# These are only distributed as pdf on sec.gov and need to be converted.

DIR="./cusips/"
FILENAME="13flist2020q3.pdf"

URL="https://www.sec.gov/divisions/investment/13f/${FILENAME}"

PDF_FILENAME="${DIR}/${FILENAME}"
TXT_FILENAME="${DIR}/${FILENAME%.pdf}.txt"
SQLITE_DB="./filings.db"

mkdir -p "${DIR}"

curl ${URL} > "${PDF_FILENAME}"

pdftotext -layout "${PDF_FILENAME}" - | \
  egrep "^[A-Z0-9]{6} [0-9]{2} [0-9] " \
  > ${TXT_FILENAME}

./parse_cusips.py "${TXT_FILENAME}" "${SQLITE_DB}"
