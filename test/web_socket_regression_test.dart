import 'dart:async';

import 'package:test/test.dart';
import 'package:web_socket_client/src/web_socket.dart' as ws_impl;
import 'package:web_socket_client/web_socket_client.dart';

import 'test_server.dart';

class _TrackingBackoff implements Backoff {
  int resetCalls = 0;
  int _step = 0;

  @override
  Duration next() {
    if (_step == 0) {
      _step++;
      return Duration.zero;
    }
    return const Duration(seconds: 1);
  }

  @override
  void reset() {
    resetCalls++;
    _step = 0;
  }
}

class _FakeWebSocketSink implements WebSocketSink {
  _FakeWebSocketSink({this.onClose});

  final Future<void> Function(int? code, String? reason)? onClose;
  final Completer<void> _done = Completer<void>();

  @override
  Future<void> addStream(Stream<Object?> stream) => stream.drain<void>();

  @override
  void add(Object? data) {}

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    if (onClose != null) {
      await onClose!(closeCode, closeReason);
    }
    if (!_done.isCompleted) {
      _done.complete();
    }
  }

  @override
  Future<void> get done => _done.future;
}

class _FakeWebSocketChannel implements WebSocketChannel {
  _FakeWebSocketChannel({
    required this.stream,
    required this.sink,
    required this.ready,
  });

  @override
  final Stream<dynamic> stream;

  @override
  final WebSocketSink sink;

  @override
  final Future<void> ready;

  @override
  String? get protocol => null;

  @override
  int? get closeCode => null;

  @override
  String? get closeReason => null;

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  final testServer = TestServer();

  setUpAll(() async {
    await testServer.setupTestServer();
  });

  tearDownAll(() async {
    await testServer.close();
  });

  group('WebSocket regressions', () {
    test('resets backoff after successful reconnect', () async {
      final backoff = _TrackingBackoff();
      final ws = WebSocket(
        testServer.uri,
        backoff: backoff,
        onMessage: (_) {},
      );
      await ws.init();
      await ws.connection.firstWhere((state) => state is Connected);

      final firstReconnected = ws.connection.firstWhere(
        (state) => state is Reconnected,
      );
      await ws.attemptToReconnect(Exception('first reconnect'));
      await firstReconnected.timeout(const Duration(milliseconds: 300));

      final secondReconnected = ws.connection.firstWhere(
        (state) => state is Reconnected,
      );
      await ws.attemptToReconnect(Exception('second reconnect'));
      await secondReconnected.timeout(const Duration(milliseconds: 300));

      // Initial connection + two successful reconnects.
      expect(backoff.resetCalls, greaterThanOrEqualTo(3));
      await ws.close();
    });

    test('preserves error and stack trace from ready failures', () async {
      final originalConnector = ws_impl.webSocketConnector;
      final originalBuilder = ws_impl.webSocketChannelBuilder;

      final readyError = StateError('ready failed');
      final readyStackTrace = StackTrace.current;
      final readyCompleter = Completer<void>();
      final fakeChannel = _FakeWebSocketChannel(
        stream: const Stream.empty(),
        sink: _FakeWebSocketSink(),
        ready: readyCompleter.future,
      );

      ws_impl.webSocketConnector =
          (
            url, {
            protocols,
            headers,
            pingInterval,
          }) async => Object();
      ws_impl.webSocketChannelBuilder = (socket) => fakeChannel;

      try {
        final ws = WebSocket(
          Uri.parse('ws://example.test'),
          backoff: NoBackoff(),
          onMessage: (_) {},
        );

        final initFuture = ws.init();
        readyCompleter.completeError(readyError, readyStackTrace);
        await initFuture;
        final state = await ws.connection.firstWhere((s) => s is Disconnected);
        final disconnected = state as Disconnected;

        expect(disconnected.error, same(readyError));
        expect(disconnected.stackTrace, same(readyStackTrace));
      } finally {
        ws_impl.webSocketConnector = originalConnector;
        ws_impl.webSocketChannelBuilder = originalBuilder;
      }
    });

    test(
      'non-string messages without onOtherMessage are ignored and keep connection',
      () async {
        final originalConnector = ws_impl.webSocketConnector;
        final originalBuilder = ws_impl.webSocketChannelBuilder;

        final streamController = StreamController<dynamic>.broadcast();
        final fakeChannel = _FakeWebSocketChannel(
          stream: streamController.stream,
          sink: _FakeWebSocketSink(),
          ready: Future<void>.value(),
        );

        ws_impl.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) async => Object();
        ws_impl.webSocketChannelBuilder = (socket) => fakeChannel;

        try {
          final ws = WebSocket(
            Uri.parse('ws://example.test'),
            backoff: NoBackoff(),
            onMessage: (_) {},
          );

          await ws.init();
          await ws.connection.firstWhere((state) => state is Connected);

          streamController.add(123);
          await Future<void>.delayed(const Duration(milliseconds: 20));

          expect(ws.connection.state, isA<Connected>());

          await ws.close();
        } finally {
          await streamController.close();
          ws_impl.webSocketConnector = originalConnector;
          ws_impl.webSocketChannelBuilder = originalBuilder;
        }
      },
    );

    test(
      'onOtherMessage handles non-string messages without reconnect',
      () async {
        final originalConnector = ws_impl.webSocketConnector;
        final originalBuilder = ws_impl.webSocketChannelBuilder;

        final streamController = StreamController<dynamic>.broadcast();
        final otherMessages = <Object?>[];
        final fakeChannel = _FakeWebSocketChannel(
          stream: streamController.stream,
          sink: _FakeWebSocketSink(),
          ready: Future<void>.value(),
        );

        ws_impl.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) async => Object();
        ws_impl.webSocketChannelBuilder = (socket) => fakeChannel;

        try {
          final ws = WebSocket(
            Uri.parse('ws://example.test'),
            backoff: NoBackoff(),
            onMessage: (_) {},
            onOtherMessage: otherMessages.add,
          );

          await ws.init();
          await ws.connection.firstWhere((state) => state is Connected);

          streamController.add(<int>[1, 2, 3]);
          await Future<void>.delayed(const Duration(milliseconds: 20));

          expect(otherMessages, hasLength(1));
          expect(otherMessages.single, equals(<int>[1, 2, 3]));
          expect(ws.connection.state, isA<Connected>());

          await ws.close();
        } finally {
          await streamController.close();
          ws_impl.webSocketConnector = originalConnector;
          ws_impl.webSocketChannelBuilder = originalBuilder;
        }
      },
    );

    test('close still finalizes when sink close and cancel throw', () async {
      final originalConnector = ws_impl.webSocketConnector;
      final originalBuilder = ws_impl.webSocketChannelBuilder;

      final errors = <Object>[];
      final streamController = StreamController<dynamic>(
        onCancel: () async {
          throw StateError('subscription cancel failed');
        },
      );
      final fakeChannel = _FakeWebSocketChannel(
        stream: streamController.stream,
        sink: _FakeWebSocketSink(
          onClose: (closeCode, closeReason) async {
            throw StateError('sink close failed');
          },
        ),
        ready: Future<void>.value(),
      );

      ws_impl.webSocketConnector =
          (
            url, {
            protocols,
            headers,
            pingInterval,
          }) async => Object();
      ws_impl.webSocketChannelBuilder = (socket) => fakeChannel;

      try {
        final ws = WebSocket(
          Uri.parse('ws://example.test'),
          backoff: NoBackoff(),
          onMessage: (_) {},
          onError: (error, _) => errors.add(error),
        );
        await ws.init();
        await ws.connection.firstWhere((state) => state is Connected);

        var connectionDone = false;
        ws.connection.listen(null, onDone: () => connectionDone = true);

        await ws.close();
        await Future<void>.delayed(Duration.zero);

        expect(ws.connection.state, isA<Disconnected>());
        expect(connectionDone, isTrue);
        expect(
          errors.whereType<StateError>().map((e) => e.message).toSet(),
          containsAll(<String>{
            'sink close failed',
            'subscription cancel failed',
          }),
        );
      } finally {
        await streamController.close();
        ws_impl.webSocketConnector = originalConnector;
        ws_impl.webSocketChannelBuilder = originalBuilder;
      }
    });
  });
}
