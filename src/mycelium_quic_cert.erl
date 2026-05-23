%%% -*- erlang -*-
%%% Copyright (c) 2026 Benoit Chesneau
%%% SPDX-License-Identifier: Apache-2.0
%%%
-module(mycelium_quic_cert).

%% X.509 Self-Signed Certificate Generation for QUIC TLS
%%
%% Generates self-signed certificates for QUIC distribution encryption.
%% Uses public_key application for certificate creation.
%%
%% The certificates are self-signed because QUIC/TLS only provides
%% encryption - actual node authentication is done via Erlang cookie
%% (handled by dist_util in erlang_quic).

-include_lib("public_key/include/public_key.hrl").

-export([
    ensure_cert/0,
    ensure_cert/1,
    generate_cert/1,
    get_cert_paths/0
]).

%% Default certificate parameters
-define(DEFAULT_DAYS, 365).
%% Backdate notBefore so a peer whose clock lags slightly does not see a
%% not-yet-valid cert.
-define(BACKDATE_SECONDS, 300).
-define(DEFAULT_CERT_DIR, "data/quic").

%%====================================================================
%% API
%%====================================================================

%% @doc Ensure QUIC certificates exist, generating them if needed.
%% Uses default directory from application config.
-spec ensure_cert() -> ok | {error, term()}.
ensure_cert() ->
    CertDir = application:get_env(mycelium, quic_cert_dir, ?DEFAULT_CERT_DIR),
    ensure_cert(CertDir).

%% @doc Ensure QUIC certificates exist in the specified directory.
-spec ensure_cert(file:filename()) -> ok | {error, term()}.
ensure_cert(CertDir) ->
    CertFile = filename:join(CertDir, "node.crt"),
    KeyFile = filename:join(CertDir, "node.key"),
    case filelib:is_regular(CertFile) andalso filelib:is_regular(KeyFile) of
        true ->
            %% Certificates exist
            ok;
        false ->
            %% Generate new certificates
            generate_cert(CertDir)
    end.

%% @doc Generate a new self-signed certificate and key.
-spec generate_cert(file:filename()) -> ok | {error, term()}.
generate_cert(CertDir) ->
    %% Ensure directory exists
    ok = filelib:ensure_dir(filename:join(CertDir, "dummy")),

    %% Generate EC key pair (P-256)
    case generate_key() of
        {ok, PrivateKey} ->
            %% Create self-signed certificate
            case create_certificate(PrivateKey) of
                {ok, Cert} ->
                    %% Write files
                    CertFile = filename:join(CertDir, "node.crt"),
                    KeyFile = filename:join(CertDir, "node.key"),
                    case write_cert_files(CertFile, KeyFile, Cert, PrivateKey) of
                        ok ->
                            logger:info("QUIC certificates generated in ~s", [CertDir]),
                            ok;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @doc Get the paths to the certificate and key files.
-spec get_cert_paths() -> {CertFile :: file:filename(), KeyFile :: file:filename()}.
get_cert_paths() ->
    CertDir = application:get_env(mycelium, quic_cert_dir, ?DEFAULT_CERT_DIR),
    {filename:join(CertDir, "node.crt"), filename:join(CertDir, "node.key")}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Generate an EC key pair on the NIST P-256 curve. The returned
%% ECPrivateKey carries the public point in its `publicKey' field.
generate_key() ->
    try
        PrivateKey = public_key:generate_key({namedCurve, ?'secp256r1'}),
        {ok, PrivateKey}
    catch
        _:Reason ->
            {error, {key_generation_failed, Reason}}
    end.

%% @private
%% Create a self-signed X.509 certificate over an EC (P-256) key.
create_certificate(PrivateKey) ->
    try
        %% Get node name for CN
        NodeName = case node() of
            nonode@nohost -> "mycelium-node";
            Node -> atom_to_list(Node)
        end,

        %% Build subject
        Subject = {rdnSequence, [
            [#'AttributeTypeAndValue'{
                type = ?'id-at-commonName',
                value = {utf8String, list_to_binary(NodeName)}
            }],
            [#'AttributeTypeAndValue'{
                type = ?'id-at-organizationName',
                value = {utf8String, <<"Mycelium">>}
            }]
        ]},

        %% Validity period. Each bound is encoded as UTCTime for
        %% years <2050 and GeneralizedTime for 2050+ per RFC 5280.
        %% notBefore is backdated a few minutes for peer clock skew.
        Now = calendar:universal_time(),
        Validity = #'Validity'{
            notBefore = validity_time(add_seconds(Now, -?BACKDATE_SECONDS)),
            notAfter  = validity_time(add_days(Now, ?DEFAULT_DAYS))
        },

        %% Serial number: positive 127-bit integer drawn from a
        %% cryptographic PRNG. Mask off the top bit so the ASN.1
        %% INTEGER encoding stays positive without an extra byte.
        SerialBytes = crypto:strong_rand_bytes(16),
        Serial = binary:decode_unsigned(SerialBytes)
                 band ((1 bsl 127) - 1),

        %% Subject public key info for an EC (P-256) key. The algorithm
        %% is id-ecPublicKey with the named-curve parameters; the public
        %% key is the raw EC point carried in the generated private key.
        #'ECPrivateKey'{publicKey = ECPoint} = PrivateKey,
        SubjectPKInfo = #'SubjectPublicKeyInfo'{
            algorithm = #'AlgorithmIdentifier'{
                algorithm = ?'id-ecPublicKey',
                %% ECParameters CHOICE value (named curve), encoded by the
                %% TBSCertificate codec; not a pre-encoded open type.
                parameters = {namedCurve, ?'secp256r1'}
            },
            subjectPublicKey = ECPoint
        },

        %% ecdsa-with-SHA256 has no algorithm parameters (absent).
        SigAlg = #'AlgorithmIdentifier'{algorithm = ?'ecdsa-with-SHA256'},

        %% TBS Certificate
        TBSCert = #'TBSCertificate'{
            version = v3,
            serialNumber = Serial,
            signature = SigAlg,
            issuer = Subject,
            validity = Validity,
            subject = Subject,
            subjectPublicKeyInfo = SubjectPKInfo,
            extensions = create_extensions()
        },

        %% Sign the certificate
        TBSDer = public_key:der_encode('TBSCertificate', TBSCert),
        Signature = public_key:sign(TBSDer, sha256, PrivateKey),

        %% Build final certificate
        Cert = #'Certificate'{
            tbsCertificate = TBSCert,
            signatureAlgorithm = SigAlg,
            signature = Signature
        },

        {ok, Cert}
    catch
        _:Reason ->
            {error, {certificate_creation_failed, Reason}}
    end.

%% @private
%% Create X.509 v3 extensions.
create_extensions() ->
    [
        %% Basic Constraints: CA:FALSE
        #'Extension'{
            extnID = ?'id-ce-basicConstraints',
            critical = true,
            extnValue = public_key:der_encode('BasicConstraints',
                #'BasicConstraints'{cA = false})
        },
        %% Key Usage: Digital Signature (EC key; no keyEncipherment).
        #'Extension'{
            extnID = ?'id-ce-keyUsage',
            critical = true,
            extnValue = public_key:der_encode('KeyUsage',
                [digitalSignature])
        },
        %% Extended Key Usage: TLS Server Auth, TLS Client Auth
        #'Extension'{
            extnID = ?'id-ce-extKeyUsage',
            critical = false,
            extnValue = public_key:der_encode('ExtKeyUsageSyntax',
                [?'id-kp-serverAuth', ?'id-kp-clientAuth'])
        }
    ].

%% @private
%% Write certificate and key to PEM files. The private key goes
%% through the atomic chmod-before-write helper so it never lives on
%% disk world-readable while it contains secret material.
write_cert_files(CertFile, KeyFile, Cert, PrivateKey) ->
    try
        CertDer = public_key:der_encode('Certificate', Cert),
        CertPem = public_key:pem_encode([{'Certificate', CertDer, not_encrypted}]),
        KeyDer = public_key:der_encode('ECPrivateKey', PrivateKey),
        KeyPem = public_key:pem_encode([{'ECPrivateKey', KeyDer, not_encrypted}]),
        ok = file:write_file(CertFile, CertPem),
        case mycelium_file:write_secure(KeyFile, KeyPem) of
            ok                 -> ok;
            {error, _} = Error -> Error
        end
    catch
        _:Reason ->
            {error, {file_write_failed, Reason}}
    end.

%% @private
%% Build an X.509 validity bound. Years < 2050 use UTCTime
%% (YYMMDDHHMMSSZ); years >= 2050 use GeneralizedTime
%% (YYYYMMDDHHMMSSZ) per RFC 5280 4.1.2.5.
validity_time({{Year, _, _}, _} = DateTime) when Year >= 2050 ->
    {generalTime, format_general_time(DateTime)};
validity_time(DateTime) ->
    {utcTime, format_utc_time(DateTime)}.

format_utc_time({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    Y = Year rem 100,
    lists:flatten(io_lib:format("~2..0w~2..0w~2..0w~2..0w~2..0w~2..0wZ",
                                [Y, Month, Day, Hour, Min, Sec])).

format_general_time({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0w~2..0w~2..0w~2..0wZ",
                                [Year, Month, Day, Hour, Min, Sec])).

%% @private
%% Add days to a datetime.
add_days(DateTime, Days) ->
    add_seconds(DateTime, Days * 24 * 60 * 60).

%% @private
%% Add (or, with a negative value, subtract) seconds to a datetime.
add_seconds(DateTime, Seconds) ->
    Base = calendar:datetime_to_gregorian_seconds(DateTime),
    calendar:gregorian_seconds_to_datetime(Base + Seconds).
