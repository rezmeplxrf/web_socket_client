import 'dart:async';

import 'package:web_socket_client/src/_web_socket_channel/_web_socket_channel_io.dart';
import 'package:web_socket_client/src/_web_socket_connect/_web_socket_connect_io.dart';
import 'package:web_socket_client/src/connection.dart';
import 'package:web_socket_client/web_socket_client.dart';

Backoff _defaultBackoff() => BinaryExponentialBackoff(
  initial: const Duration(milliseconds: 100),
  maximumStep: 10,
);

/// The default connection timeout duration.
const _defaultTimeout = Duration(seconds: 30);

/// {@template web_socket}
/// A reusable WebSocket client for Dart.
/// {@endtemplate}
class WebSocket {
  /// {@macro web_socket}
  WebSocket(
    Uri uri, {
    required void Function(String message) onMessage,
    void Function(Object error, StackTrace stackTrace)? onError,
    Iterable<String>? protocols,
    Duration? pingInterval,
    Map<String, dynamic>? headers,
    Backoff? backoff,
    Duration? timeout,
  }) : _uri = uri,
       _onMessage = onMessage,
       _onError = onError,
       _protocols = protocols,
       _pingInterval = pingInterval,
       _headers = headers,
       _backoff = backoff ?? _defaultBackoff(),
       _timeout = timeout ?? _defaultTimeout;
  final void Function(String message) _onMessage;
  final void Function(Object error, StackTrace stackTrace)? _onError;
  final Uri _uri;
  final Iterable<String>? _protocols;
  final Map<String, dynamic>? _headers;
  final Duration? _pingInterval;
  final Backoff _backoff;
  final Duration _timeout;

  final _connectionController = ConnectionController();
  StreamSubscription<dynamic>? _subscription;

  Timer? _backoffTimer;

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

  Future<void>? _initFuture;

  Future<void> attemptToReconnect([
    Object? error,
    StackTrace? stackTrace,
  ]) async {
    if (_isClosedByClient) return;
    switch (_connectionController.state) {
      case Disconnecting():
      case Disconnected():
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

    // If NoBackoff is used, do not attempt to reconnect.
    if (_backoff is NoBackoff) {
      await close();
      return;
    }
    await _reconnect();
  }

  void _reportError(Object error, [StackTrace? stackTrace]) {
    _onError?.call(error, stackTrace ?? StackTrace.empty);
  }

  Future<void> init({String? onReady}) {
    if (_isConnected) return Future.value();

    if (_initFuture != null) {
      if (onReady != null) {
        return _initFuture!.whenComplete(() {
          if (_isConnected) {
            _channel?.sink.add(onReady);
          }
        });
      }
      return _initFuture!;
    }

    _initFuture = _performInit(onReady: onReady);
    return _initFuture!;
  }

  Future<void> _performInit({String? onReady}) async {
    if (_isConnected) {
      return;
    }
    if (_isClosedByClient) {
      await close();
      return;
    }

    try {
      try {
        if (_channel != null) {
          await _subscription?.cancel();
          await _channel?.sink.close();
        }
        _channel = null;
        _subscription = null;
        final ws = await connect(
          _uri.toString(),
          protocols: _protocols,
          headers: _headers,
          pingInterval: _pingInterval,
        ).timeout(_timeout);

        if (_isClosedByClient) {
          await ws.close();
          return;
        }

        _channel = getWebSocketChannel(ws);
      } catch (error, stackTrace) {
        await attemptToReconnect(error, stackTrace);
        return;
      }

      _subscription = _channel?.stream.listen(
        (msg) {
          if (msg is String) {
            _onMessage(msg);
          } else if (_onError != null) {
            _reportError(
              StateError(
                'Unexpected message type "${msg.runtimeType}" received from WebSocket stream.',
              ),
            );
          }
        },
        onDone: attemptToReconnect,
        cancelOnError: true,
        onError: (Object error, StackTrace stacktrace) async {
          await attemptToReconnect(error, stacktrace);
        },
      );

      try {
        await _channel?.ready;
      } catch (e) {
        await attemptToReconnect(Exception('Connection Timeout: $e'));
        return;
      }

      switch (_connectionController.state) {
        case Reconnecting():
          _connectionController.add(const Reconnected());
        case Connecting():
          _connectionController.add(const Connected());
        default:
      }
      if (onReady != null) {
        _channel?.sink.add(onReady);
      }
    } finally {
      _initFuture = null;
    }
  }

  Future<void> _reconnect() async {
    if (_isClosedByClient || _isConnected) return;
    if (_backoff is NoBackoff) return;
    _connectionController.add(const Reconnecting());
    _backoffTimer?.cancel();
    _backoffTimer = Timer(_backoff.next(), () async {
      await init();
    });
  }

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
  bool send(String message) {
    if (_channel != null && _channel?.closeCode == null) {
      try {
        _channel?.sink.add(message);
        return true;
      } catch (error, stackTrace) {
        _reportError(error, stackTrace);
        return false;
      }
    } else {
      _reportError(
        StateError('Cannot send WebSocket message while disconnected.'),
      );
      return false;
    }
  }

  /// Closes the connection and frees any resources.
  Future<void> close([int? code, String? reason]) async {
    if (_isClosedByClient) return;
    try {
      _isClosedByClient = true;
      _backoffTimer?.cancel();
      _backoff.reset();
      if (_isConnected) _connectionController.add(const Disconnecting());
      await _channel?.sink.close(code, reason);
      await _subscription?.cancel();
      _subscription = null;
      _channel = null;
      if (_connectionController.state is! Disconnected) {
        _connectionController.add(Disconnected(code: code, reason: reason));
      }
      _connectionController.close();
    } catch (error, stackTrace) {
      _reportError(error, stackTrace);
    }
  }
}
