#!/bin/bash
#
# Run a quickstart node locally. Each node gets its own identity (keys)
# and TLS cert under data/node<N>/, but shares data/discovery so the file
# discovery backend lets them find each other on one host.
#
#   Terminal 1:  ./scripts/run-local.sh 1     # the seed
#   Terminal 2:  ./scripts/run-local.sh 2     # joins node1 automatically
#
# Then in node2's shell:
#   quickstart:peers().            %% ['node1@<host>']
#   quickstart:work(hello).        %% handled locally on node2
#   quickstart:work_on('node1@<host>', hello).   %% routed to node1
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
cd "$APP_DIR"

HOST=$(hostname -s)
N="${1:-1}"
PORT=$((9100 + N))
DATA="data/node${N}"
mkdir -p "${DATA}/keys" "${DATA}/quic" data/discovery

# Build offline against this repo by linking it as a checkout.
if [ ! -L "_checkouts/mycelium" ]; then
    mkdir -p _checkouts
    ln -sf ../../.. _checkouts/mycelium
fi

DIST="-proto_dist mycelium -epmd_module mycelium_epmd -start_epmd false -mycelium_dist_port ${PORT}"
DIRS="-mycelium auth_key_dir \"${DATA}/keys\" -mycelium quic_cert_dir \"${DATA}/quic\" -mycelium discovery_dir \"data/discovery\""

if [ "$N" = "1" ]; then
    EVAL="io:format(\"node1 ready; start node 2 in another terminal~n\")."
else
    EVAL="timer:sleep(800), io:format(\"join node1: ~p~n\", [mycelium:join('node1@${HOST}')])."
fi

ERL_AFLAGS="${DIST} ${DIRS}" \
    rebar3 shell --sname "node${N}" --setcookie quickstart --eval "${EVAL}"
