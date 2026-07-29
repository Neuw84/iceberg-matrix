#!/usr/bin/env bash
#
# Shared helper: detect the address of this host that both containers and
# host-side processes can use to reach published ports.
#
# Why this exists: Iceberg's Java REST client lets the storage config the
# catalog returns on loadTable override the client's own, so every engine ends
# up using whatever S3 endpoint the warehouse advertises. A compose-internal
# name like http://minio:9000 is therefore unusable, because host-side engines
# cannot resolve it. The host's own IP works from both sides -- containers reach
# it through the published port, and the host reaches itself -- with no
# /etc/hosts entry and no root access.
#
# Source this file and call detect_host_ip.

detect_host_ip() {
  local ip=""
  if command -v ipconfig >/dev/null 2>&1; then
    for iface in en0 en1 en2 en3; do
      ip="$(ipconfig getifaddr "${iface}" 2>/dev/null || true)"
      [[ -n "${ip}" ]] && break
    done
  fi
  if [[ -z "${ip}" ]]; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"
  fi
  if [[ -z "${ip}" ]]; then
    # No traffic is sent; this just asks the kernel which local address would
    # be used to reach an external host.
    ip="$(python3 -c 'import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
finally:
    s.close()' 2>/dev/null || true)"
  fi
  printf '%s' "${ip}"
}
