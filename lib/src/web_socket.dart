// ignore_for_file: avoid_print

import 'dart:async';
import 'dart:developer' as developer;

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

  Completer<void>? _initCompleter;

  bool get _isConnected =>
      _connectionController.state is Connected ||
      _connectionController.state is Reconnected;

  bool get _isConnecting =>
      _connectionController.state is Connecting ||
      _connectionController.state is Reconnecting;

  bool _isClosedByClient = false;

  void attemptToReconnect([Object? error, StackTrace? stackTrace]) {
    // Prevent reconnection attempts if already connecting or closed
    if (_isConnecting || _isClosedByClient) {
      return;
    }

    _connectionController.add(
      Disconnected(
        code: _channel?.closeCode,
        reason: _channel?.closeReason,
        error: error,
        stackTrace: stackTrace,
      ),
    );

    // Check state after adding disconnected state
    switch (_connectionController.state) {
      case Disconnecting():
      case Reconnecting():
        return;
      default:
    }

    if (_backoffDuration >= _timeout) {
      return _closeWithTimeout();
    }

    // If NoBackoff is used, do not attempt to reconnect.
    if (_backoff is NoBackoff) {
      close();
      return;
    }
    _reconnect();
  }

  Future<void> init({bool isReconnection = false}) async {
    final connectionState = _connectionController.state;
    switch (connectionState) {
      case Connecting():
      case Reconnecting():
        return _initCompleter?.future ?? Future.value();
      case Connected():
      case Reconnected():
        return;

      default:
    }
    if (isReconnection) {
      if (_isClosedByClient) return;
      _connectionController.add(const Reconnecting());
    } else {
      _connectionController.add(const Connecting());
    }
    _initCompleter = Completer<void>();

    try {
      final ws = await connect(
        _uri.toString(),
        protocols: _protocols,
        headers: _headers,
        pingInterval: _pingInterval,
        binaryType: _binaryType,
      ).timeout(_timeout);

      // Check if closed during connection attempt
      if (_isClosedByClient) {
        await close();
        return;
      }

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
      await _channel!.ready;

      // Check again if closed during ready wait
      if (_isClosedByClient) {
        await _channel?.sink.close();
        return;
      }

      developer.log(
          'WebSocket connection established with protocols: ${_channel?.protocol}',
          name: 'WebSocket');

      // Reset backoff duration and strategy on successful connection
      _backoffDuration = Duration.zero;
      _backoff.reset();
      _backoffTimer?.cancel();

      final connectionState = _connectionController.state;
      switch (connectionState) {
        case Reconnecting():
          _connectionController.add(const Reconnected());
        case Connecting():
          _connectionController.add(const Connected());
        default:
      }
    } catch (error, stackTrace) {
      attemptToReconnect(error, stackTrace);
    } finally {
      _initCompleter?.complete();
      _initCompleter = null;
    }
  }

  Future<void> _reconnect() async {
    if (_isConnecting || _isClosedByClient || _isConnected) {
      return;
    }
    if (_backoff is NoBackoff) {
      return;
    }

    final next = _backoff.next();
    if (_backoffDuration + next >= _timeout) {
      return _closeWithTimeout();
    }
    // Cancel existing timer before setting new one
    _backoffTimer?.cancel();
    _backoffDuration = _backoffDuration + next;

    _backoffTimer = Timer(next, () async {
      if (!_isClosedByClient && !_isConnected) {
        await init(isReconnection: true);
      }
    });
  }

  static const int _timeoutCloseCode = 1006;
  static const String _timeoutCloseReason = 'connection timeout';

  void _closeWithTimeout() => close(_timeoutCloseCode, _timeoutCloseReason);

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
  Future<void> close([int? code, String? reason]) async {
    if (_isClosedByClient) {
      return;
    }
    developer.log('Closing WebSocket connection: $code, $reason',
        name: 'WebSocket');
    _isClosedByClient = true;

    // Cancel backoff timer and ongoing operations
    _backoffTimer?.cancel();
    _backoffTimer = null;
    _backoffDuration = Duration.zero;

    // Complete any pending init operations
    if (_initCompleter != null && !_initCompleter!.isCompleted) {
      _initCompleter!.complete();
    }

    if (_isConnected) {
      _connectionController.add(const Disconnecting());
    }

    await _channel?.sink.close(code, reason);
    await _subscription?.cancel();
    _subscription = null;
    _channel = null;

    _connectionController
      ..add(Disconnected(code: code, reason: reason))
      ..close();
  }
}
