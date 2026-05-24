%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% EUnit tests for mycelium_quic_cert validity-date encoding and
%%% serial-number derivation.

-module(mycelium_quic_cert_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("kernel/include/file.hrl").

%%====================================================================
%% Helpers
%%====================================================================

tmp_dir() ->
    Dir = filename:join([
        "/tmp",
        "mycelium_quic_cert_tests",
        integer_to_list(erlang:unique_integer([positive]))
    ]),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    Dir.

cleanup(Dir) ->
    os:cmd("rm -rf " ++ Dir),
    ok.

%% Extract the X.509 Validity record from a PEM-encoded cert file.
read_validity(CertFile) ->
    {ok, Pem} = file:read_file(CertFile),
    [{'Certificate', Der, not_encrypted} | _] = public_key:pem_decode(Pem),
    Cert = public_key:der_decode('Certificate', Der),
    TBS = Cert#'Certificate'.tbsCertificate,
    TBS#'TBSCertificate'.validity.

read_serial(CertFile) ->
    {ok, Pem} = file:read_file(CertFile),
    [{'Certificate', Der, not_encrypted} | _] = public_key:pem_decode(Pem),
    Cert = public_key:der_decode('Certificate', Der),
    TBS = Cert#'Certificate'.tbsCertificate,
    TBS#'TBSCertificate'.serialNumber.

%%====================================================================
%% Tests
%%====================================================================

%% Default validity (10 years from now) lands well before 2050 so
%% both bounds should be UTCTime today. This locks down that we did
%% not break the common case while adding GeneralizedTime support.
default_validity_uses_utc_time_today_test() ->
    Dir = tmp_dir(),
    try
        ok = mycelium_quic_cert:ensure_cert(Dir),
        CertFile = filename:join(Dir, "node.crt"),
        Validity = read_validity(CertFile),
        ?assertMatch({utcTime, _}, Validity#'Validity'.notBefore),
        ?assertMatch({utcTime, _}, Validity#'Validity'.notAfter)
    after
        cleanup(Dir)
    end.

%% The serial number is a positive integer at least 64 bits wide.
cert_serial_is_at_least_64_bits_test() ->
    Dir = tmp_dir(),
    try
        ok = mycelium_quic_cert:ensure_cert(Dir),
        CertFile = filename:join(Dir, "node.crt"),
        Serial = read_serial(CertFile),
        ?assert(is_integer(Serial)),
        ?assert(Serial > 0),
        ?assert(Serial >= (1 bsl 63))
    after
        cleanup(Dir)
    end.

%% Two consecutive cert generations produce distinct serials. A
%% non-CSPRNG (rand:uniform/1) would also satisfy this on most days;
%% this is a statistical sanity check, not a randomness proof.
serials_are_distinct_across_generations_test() ->
    Dir1 = tmp_dir(),
    Dir2 = tmp_dir(),
    try
        ok = mycelium_quic_cert:ensure_cert(Dir1),
        ok = mycelium_quic_cert:ensure_cert(Dir2),
        S1 = read_serial(filename:join(Dir1, "node.crt")),
        S2 = read_serial(filename:join(Dir2, "node.crt")),
        ?assertNotEqual(S1, S2)
    after
        cleanup(Dir1),
        cleanup(Dir2)
    end.

%% The TLS private key file is created with 0600 perms.
key_file_mode_is_0600_test() ->
    Dir = tmp_dir(),
    try
        ok = mycelium_quic_cert:ensure_cert(Dir),
        KeyFile = filename:join(Dir, "node.key"),
        {ok, FI} = file:read_file_info(KeyFile),
        ?assertEqual(8#600, FI#file_info.mode band 8#777)
    after
        cleanup(Dir)
    end.
