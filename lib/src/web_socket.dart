import 'dart:async';

import 'package:web_socket_client/src/_web_socket_channel/_web_socket_channel.dart'
    if (dart.library.io) 'package:web_socket_client/src/_web_socket_channel/_web_socket_channel_io.dart'
    if (dart.library.js_interop) 'package:web_socket_client/src/_web_socket_channel/_web_socket_channel_html.dart';
import 'package:web_socket_client/src/_web_socket_connect/_web_socket_connect.dart'
    if (dart.library.io) 'package:web_socket_client/src/_web_socket_connect/_web_socket_connect_io.dart'
    if (dart.library.js_interop) 'package:web_socket_client/src/_web_socket_connect/_web_socket_connect_html.dart';
import 'package:web_socket_client/src/connection.dart';
import 'package:web_socket_client/web_socket_client.dart';

/// The default backoff strategy.
final _defaultBackoff = BinaryExponentialBackoff(
  initial: const Duration(milliseconds: 100),
  maximumStep: 7,
);

/// The default connection timeout duration.
const _defaultTimeout = Duration(seconds: 60);

/// {@template web_socket}
/// A reusable WebSocket client for Dart.
/// {@endtemplate}
class WebSocket {
  /// {@macro web_socket}
  WebSocket(Uri uri,
      {required void Function(String message) onMessage,
      Iterable<String>? protocols,
      Duration? pingInterval,
      Map<String, dynamic>? headers,
      Backoff? backoff,
      Duration? timeout,
      String? binaryType})
      : _uri = uri,
        _onMessage = onMessage,
        _protocols = protocols,
        _pingInterval = pingInterval,
        _headers = headers,
        _backoff = backoff ?? _defaultBackoff,
        _timeout = timeout ?? _defaultTimeout,
        _binaryType = binaryType;
  final void Function(String message) _onMessage;
  final Uri _uri;
  final Iterable<String>? _protocols;
  final Map<String, dynamic>? _headers;
  final Duration? _pingInterval;
  final Backoff _backoff;
  final Duration _timeout;
  final String? _binaryType;

  final _connectionController = ConnectionController();
  StreamSubscription<dynamic>? _subscription;

  Timer? _backoffTimer;
  Duration _backoffDuration = Duration.zero;

  WebSocketChannel? _channel;

  bool get _isConnected {
    switch (_connectionController.state) {
      case Connected():
      case Reconnected():
        return true;
      default:
        return false;
    }
  }

  bool _isClosedByClient = false;

  void attemptToReconnect([Object? error, StackTrace? stackTrace]) {
    if (_isClosedByClient) {
      return;
    }

    switch (_connectionController.state) {
      case Disconnecting():
      case Reconnecting():
        return;
      default:
    }
    _connectionController.add(
      Disconnected(
        code: _channel?.closeCode,
        reason: _channel?.closeReason,
        error: error,
        stackTrace: stackTrace,
      ),
    );

    if (_backoffDuration >= _timeout) return _closeWithTimeout();

    _channel = null;
    // If NoBackoff is used, do not attempt to reconnect.
    if (_backoff is NoBackoff) {
      _subscription?.cancel();
      _connectionController.close();
      _isClosedByClient = true;
      _backoffTimer?.cancel();
      _backoffDuration = Duration.zero;

      return;
    }
    _reconnect();
  }

  Future<void> init() async {
    if (_isConnected) return;

    try {
      final ws = await connect(
        _uri.toString(),
        protocols: _protocols,
        headers: _headers,
        pingInterval: _pingInterval,
        binaryType: _binaryType,
      ).timeout(_timeout);

      _channel = getWebSocketChannel(ws);

      _subscription?.cancel().ignore();
      _subscription = _channel!.stream.distinct().listen(
        (msg) {
          if (msg == null || msg is! String) {
            return;
          }
          _onMessage(msg);
        },
        onDone: attemptToReconnect,
        cancelOnError: true,
      );
      unawaited(_channel!.ready.whenComplete(() {
        final connectionState = _connectionController.state;
        switch (connectionState) {
          case Reconnecting():
            _connectionController.add(const Reconnected());
          case Connecting():
            _connectionController.add(const Connected());
          default:
        }
      }));
    } catch (error, stackTrace) {
      attemptToReconnect(error, stackTrace);
    }
  }

  Future<void> _reconnect() async {
    if (_backoffDuration >= _timeout) return _closeWithTimeout();
    if (_isClosedByClient || _isConnected) return;
    if (_backoff is NoBackoff) return;

    _connectionController.add(const Reconnecting());

    await init();

    if (_isClosedByClient || _isConnected) {
      _backoff.reset();
      _backoffTimer?.cancel();
      _backoffDuration = Duration.zero;
      return;
    }

    _backoffTimer?.cancel();
    final next = _backoff.next();
    _backoffDuration = _backoffDuration + next;
    _backoffTimer = Timer(next, _reconnect);
  }

  void _closeWithTimeout() => close(1006, 'connection timeout');

  /// The WebSocket [Connection].
  Connection get connection => _connectionController;

  /// The subprotocol selected by the server.
  ///
  /// This is initially empty. After the connection is established the value is
  /// set to the subprotocol selected by the server. If no subprotocol is
  /// negotiated the value will remain empty.
  String get protocol => _channel?.protocol ?? '';

  /// Enqueues the specified data to be transmitted
  /// to the server over the WebSocket connection.
  void send(String message) => _channel?.sink.add(message);

  /// Closes the connection and frees any resources.
  void close([int? code, String? reason]) {
    if (_isClosedByClient) return;
    _isClosedByClient = true;
    _backoffTimer?.cancel();
    _backoffDuration = Duration.zero;
    if (_isConnected) _connectionController.add(const Disconnecting());
    Future.wait<void>([
      if (_channel != null) _channel!.sink.close(code, reason),
    ]).whenComplete(() {
      _connectionController.add(Disconnected(code: code, reason: reason));
      _subscription?.cancel();
      _connectionController.close();
    });
  }
}
