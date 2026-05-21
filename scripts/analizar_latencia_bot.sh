#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="${1:-bot_hft.log}"
if [[ ! -f "$LOG_FILE" ]]; then
  echo "Uso: $0 <archivo_log>"
  exit 1
fi

awk '
/LAT_REST path=/ {
  send=recv=0;
  for(i=1;i<=NF;i++){
    if($i ~ /^send_ms=/){split($i,a,"="); send=a[2]+0}
    if($i ~ /^recv_ms=/){split($i,b,"="); recv=b[2]+0}
  }
  n++; s_sum+=send; r_sum+=recv;
  if(send>s_max)s_max=send;
  if(recv>r_max)r_max=recv;
}
END {
  if(n==0){
    print "No se encontraron lineas LAT_REST en el log.";
    exit 2;
  }
  printf("Muestras: %d\n", n);
  printf("Send  avg: %.3f ms | max: %.3f ms\n", s_sum/n, s_max);
  printf("Recv  avg: %.3f ms | max: %.3f ms\n", r_sum/n, r_max);
  printf("Round avg: %.3f ms | max: %.3f ms\n", (s_sum+r_sum)/n, s_max+r_max);
}' "$LOG_FILE"
