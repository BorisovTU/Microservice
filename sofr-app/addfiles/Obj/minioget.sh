#!/bin/bash
# Example: ./minioget.sh https://url username password bucket-name minio/path/to/file.txt /download/path/to/file.txt
set -e
# set -x

function log(){
    echo "$(date -u +'%Y%m%dT%H%M%SZ') - $*"
}

function rawurlencode() {
  local LC_ALL=C
  for (( i = 0; i < ${#1}; i++ )); do
    : "${1:i:1}"
    case "$_" in
      [/a-zA-Z0-9.~_-])
        printf '%s' "$_"
      ;;

      *)
        printf '%%%02X' "'$_"
      ;;
    esac
  done
  printf '\n'
}

if [ -z $1 ]; then
  echo "Не указан MINIO URL!"
  exit 1
fi

if [ -z $2 ]; then
  echo "Не указан пользователь!"
  exit 1
fi

if [ -z $3 ]; then
  echo "Не указан пароль!"
  exit 1
fi

if [ -z $4 ]; then
  echo "Не указана корзина!"
  exit 1
fi

if [ -z "$5" ]; then
  echo "Не указан путь к файлу MINIO!"
  exit 1
fi

if [ -z "$6" ]; then
  echo "Не указан путь сохранения!"
  exit 1
fi

log $1 $2
log $4 $5
log $6
# variable declaration - start
endpoint=$1
host=$(echo "${endpoint}" | cut -d'/' -f3 | cut -d':' -f1-2)
host=$(echo "${host}" | cut -d':' -f1)
access_key_id=$2
secret_access_key=$3
bucket=$4
amzFile=$5
amzFile=$(rawurlencode "$amzFile")
aws_region="us-east-1"
outputFile=$6
dateValueS=$(date -u +'%Y%m%d')
dateValueL=$(date -u +'%Y%m%dT%H%M%SZ')
#emptySha=`echo -n ""|sha256sum|sed 's/\s*\*-//'` #windows
emptySha=`echo -n ""|sha256sum|sed 's/\s*-//'`  #Linux
# variable declaration - end

log "downloading $amzFile"
# getting file form s3 - start
#creating a canonical request
log "creating canonical request"
log ${emptySha}

canonicalRequest="GET\n/${bucket}/${amzFile}\n\n"\
"host:${host}\n"\
"x-amz-content-sha256:${emptySha}""\n"\
"x-amz-date:${dateValueL}""\n\n"\
"host;x-amz-content-sha256;x-amz-date\n"\
"${emptySha}"
canonicalRequestHash=`/bin/echo -en "$canonicalRequest" | openssl sha256 -binary | xxd -p -c256`
log ${canonicalRequestHash}

#creating string to sign
log "creating string to sign"
stringToSign="AWS4-HMAC-SHA256\n${dateValueL}\n${dateValueS}/us-east-1/s3/aws4_request\n${canonicalRequestHash}"

function hmac_sha256 {
 key="$1"
 data="$2"
  echo -n "$data" | openssl dgst -sha256 -mac HMAC -macopt "$key" | sed 's/^.* //'
}

#creating an authorization string
log "creating auth string"
dateKey=$(hmac_sha256 key:"AWS4$secret_access_key" $dateValueS)
dateRegionKey=$(hmac_sha256 hexkey:$dateKey $aws_region)
dateRegionServiceKey=$(hmac_sha256 hexkey:$dateRegionKey s3)
signingKey=$(hmac_sha256 hexkey:$dateRegionServiceKey "aws4_request")
signature=`/bin/echo -en $stringToSign | openssl dgst -sha256 -mac HMAC -macopt hexkey:$signingKey -binary | xxd -p -c256`

#curl to s3 to get the file
log "curling to https://${bucket}.s3.amazonaws.com/${amzFile}"
curl -k -H "Authorization: AWS4-HMAC-SHA256 Credential=${access_key_id}/${dateValueS}/${aws_region}/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=${signature}" \
 -H "Accept-Encoding: zstd,gzip" \
 -H "host: ${host}" \
 -H "x-amz-content-sha256: $emptySha" \
 -H "x-amz-date: ${dateValueL}"\
 ${endpoint}/${bucket}/${amzFile} -o "$outputFile"
