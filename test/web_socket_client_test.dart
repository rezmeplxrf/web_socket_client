// ignore_for_file: unnecessary_lambdas, prefer_const_constructors, unawaited_futures

import 'dart:async';

import 'package:schedulers/schedulers.dart';
import 'package:test/test.dart';
import 'package:web_socket_client/src/web_socket.dart' as internal_ws;
import 'package:web_socket_client/web_socket_client.dart';

import 'test_server.dart';

void main() {
  final testServer = TestServer();
  setUpAll(() async {
    await testServer.setupTestServer();
  });
  tearDownAll(() async {
    await testServer.close();
  });

  Future<void> waitFor(
    bool Function() predicate, {
    Duration timeout = const Duration(seconds: 2),
    Duration interval = const Duration(milliseconds: 10),
  }) async {
    final end = DateTime.now().add(timeout);
    while (DateTime.now().isBefore(end)) {
      if (predicate()) return;
      await Future<void>.delayed(interval);
    }
    fail('Condition not met within $timeout.');
  }

  group('WebSocket', () {
    late WebSocket ws;
    final messages = <String>[];

    setUp(() async {
      messages.clear();
      ws = WebSocket(
        testServer.uri,
        onMessage: (msg) => messages.add(msg),
      );
      await ws.init();
      // Wait for connection to establish
      await ws.connection.firstWhere((state) => state is Connected);
    });

    tearDown(() async {
      await ws.close();
    });

    test('connects and receives echo', () async {
      ws.send('hello');
      await waitFor(() => messages.contains('echo hello'));
      expect(messages, contains('echo hello'));
    });

    test('reconnects after server closes connection', () async {
      ws.send('test');
      await waitFor(() => messages.contains('echo test'));
      // Simulate server closing connection by closing client channel
      await ws.close();
      // Re-initialize to simulate reconnect
      ws = WebSocket(
        testServer.uri,
        onMessage: (msg) => messages.add(msg),
      );
      await ws.init();
      await ws.connection.firstWhere((state) => state is Connected);
      ws.send('again');
      await waitFor(() => messages.contains('echo again'));
      expect(messages, contains('echo again'));
    });

    test('connection state transitions', () async {
      expect(ws.connection.state, isA<Connected>());
      await ws.close();
      expect(ws.connection.state, isA<Disconnected>());
    });
  });

  group('Backoff strategies', () {
    test('ConstantBackoff returns the same duration every time', () {
      final backoff = ConstantBackoff(const Duration(seconds: 1));
      expect(backoff.next(), const Duration(seconds: 1));
      expect(backoff.next(), const Duration(seconds: 1));
      expect(backoff.next(), const Duration(seconds: 1));
      backoff.reset();
      expect(backoff.next(), const Duration(seconds: 1));
    });

    test('ConstantBackoff handles zero durations', () {
      final zeroBackoff = ConstantBackoff(Duration.zero);
      expect(zeroBackoff.next(), Duration.zero);
      zeroBackoff.reset();
      expect(zeroBackoff.next(), Duration.zero);
    });

    test('ConstantBackoff asserts on negative durations when used', () {
      expect(
        () => ConstantBackoff(const Duration(seconds: -1)).next(),
        throwsA(isA<AssertionError>()),
      );
    });

    test('BinaryExponentialBackoff doubles duration up to maximumStep', () {
      final backoff = BinaryExponentialBackoff(
        initial: const Duration(milliseconds: 100),
        maximumStep: 4,
      );
      expect(backoff.next(), const Duration(milliseconds: 100)); // step 1
      expect(backoff.next(), const Duration(milliseconds: 200)); // step 2
      expect(backoff.next(), const Duration(milliseconds: 400)); // step 3
      expect(backoff.next(), const Duration(milliseconds: 800)); // step 4
      // Should not exceed maximumStep, stays at 800ms
      expect(backoff.next(), const Duration(milliseconds: 800));
      expect(backoff.next(), const Duration(milliseconds: 800));
      backoff.reset();
      expect(backoff.next(), const Duration(milliseconds: 100));
    });

    test(
      'BinaryExponentialBackoff with maximumStep 1 returns initial always',
      () {
        final backoff = BinaryExponentialBackoff(
          initial: const Duration(milliseconds: 50),
          maximumStep: 1,
        );
        expect(backoff.next(), const Duration(milliseconds: 50));
        expect(backoff.next(), const Duration(milliseconds: 50));
        expect(backoff.next(), const Duration(milliseconds: 50));
      },
    );

    test('BinaryExponentialBackoff asserts when maximumStep <= 0', () {
      expect(
        () => BinaryExponentialBackoff(
          initial: const Duration(milliseconds: 10),
          maximumStep: 0,
        ),
        throwsA(isA<AssertionError>()),
      );
    });

    test('BinaryExponentialBackoff handles zero initial durations', () {
      final zeroBackoff = BinaryExponentialBackoff(
        initial: Duration.zero,
        maximumStep: 3,
      );
      expect(zeroBackoff.next(), Duration.zero);
      expect(zeroBackoff.next(), Duration.zero);
    });

    test('BinaryExponentialBackoff asserts on negative initial durations', () {
      expect(
        () => BinaryExponentialBackoff(
          initial: const Duration(seconds: -1),
          maximumStep: 2,
        ),
        throwsA(isA<AssertionError>()),
      );
    });

    test('NoBackoff always returns Duration.zero and does not retry', () {
      final backoff = NoBackoff();
      expect(backoff.next(), Duration.zero);
      expect(backoff.next(), Duration.zero);
      backoff.reset();
      expect(backoff.next(), Duration.zero);
    });
  });

  group('WebSocket edge cases', () {
    test('send after close does not throw', () async {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
      );
      await ws.init();
      await ws.close();
      expect(() => ws.send('should not throw'), returnsNormally);
    });

    test('double close does not throw', () async {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
      );
      await ws.init();
      await ws.close();
      expect(() => ws.close(), returnsNormally);
    });

    test('connection with invalid URL transitions to Disconnected', () async {
      final ws = WebSocket(
        Uri.parse('ws://invalid:12345'),
        onMessage: (_) {},
        timeout: const Duration(milliseconds: 200),
        backoff: NoBackoff(),
      );
      await ws.init();
      // Wait for state to become Disconnected
      final state = await ws.connection.firstWhere((s) => s is Disconnected);
      expect(state, isA<Disconnected>());
      await ws.close();
    });

    test('reconnect does not race with close', () async {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
        timeout: const Duration(milliseconds: 200),
      );
      await ws.init();
      // Simulate abrupt disconnect by closing the underlying channel
      await ws.connection.firstWhere((s) => s is Connected);
      // Force a reconnect attempt
      ws.attemptToReconnect(Exception('test disconnect'));
      // Immediately call close
      await ws.close();
      // Wait a bit to allow any pending reconnects to fire
      await Future<void>.delayed(const Duration(milliseconds: 50));
      // Should remain closed and not reconnect
      expect(ws.connection.state, isA<Disconnected>());
    });

    test('close during reconnect prevents further reconnects', () async {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
        timeout: const Duration(milliseconds: 200),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);
      // Simulate disconnect and trigger reconnect
      ws.attemptToReconnect(Exception('trigger reconnect'));
      // Wait for reconnecting state
      await ws.connection.firstWhere((s) => s is Reconnecting);
      // Call close during reconnect
      await ws.close();
      // Wait a bit to ensure no reconnect happens
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(ws.connection.state, isA<Disconnected>());
    });

    test('close is idempotent and cancels reconnect timer', () async {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
        timeout: const Duration(milliseconds: 200),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);
      ws.attemptToReconnect(Exception('trigger reconnect'));
      await ws.close();
      // Call close again, should not throw
      expect(() => ws.close(), returnsNormally);
      // Wait to ensure no reconnect occurs
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(ws.connection.state, isA<Disconnected>());
    });

    test('handles SocketException by triggering reconnect', () async {
      final messages = <String>[];
      final ws = WebSocket(
        testServer.uri,
        onMessage: (msg) => messages.add(msg),
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);

      // Simulate SocketException by calling attemptToReconnect with SocketException
      final socketException = Exception(
        'SocketException: Reading from a closed socket',
      );
      ws.attemptToReconnect(socketException);

      // Wait for reconnecting state
      await ws.connection.firstWhere((s) => s is Reconnecting);
      expect(ws.connection.state, isA<Reconnecting>());

      await ws.close();
    });

    test('ignores messages when channel is closed', () async {
      final messages = <String>[];
      final ws = WebSocket(
        testServer.uri,
        onMessage: (msg) => messages.add(msg),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);

      // Send a message to verify connection works
      ws.send('test');
      await waitFor(() => messages.contains('echo test'));
      expect(messages, contains('echo test'));

      // Close the connection
      await ws.close();

      // Verify connection is closed
      expect(ws.connection.state, isA<Disconnected>());

      // Try to send message after close - should not crash or add to messages
      final initialMessageCount = messages.length;
      ws.send('should be ignored');
      await Future<void>.delayed(const Duration(milliseconds: 100));

      // Message count should remain the same
      expect(messages.length, equals(initialMessageCount));
    });

    test('safely handles SocketException from closed stream', () async {
      final messages = <String>[];
      final ws = WebSocket(
        testServer.uri,
        onMessage: (msg) => messages.add(msg),
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);

      // Verify initial connection works
      ws.send('test');
      await waitFor(() => messages.contains('echo test'));
      expect(messages, contains('echo test'));

      // Simulate SocketException from reading a closed stream
      final socketException = Exception('SocketException: Connection closed');
      ws.attemptToReconnect(socketException);

      // Should transition to reconnecting state without crashing
      await ws.connection.firstWhere((s) => s is Reconnecting);
      expect(ws.connection.state, isA<Reconnecting>());

      // Should eventually reconnect successfully
      await ws.connection.firstWhere((s) => s is Reconnected);
      expect(ws.connection.state, isA<Reconnected>());

      // Verify connection still works after handling the exception
      ws.send('after exception');
      await waitFor(() => messages.contains('echo after exception'));
      expect(messages, contains('echo after exception'));

      await ws.close();
    });

    test('init with onReady sends startup payload', () async {
      final messages = <String>[];
      final ws = WebSocket(
        testServer.uri,
        onMessage: messages.add,
      );

      await ws.init(onReady: 'ready');
      await ws.connection.firstWhere((s) => s is Connected);
      await waitFor(() => messages.contains('echo ready'));
      expect(messages, contains('echo ready'));
      await ws.close();
    });

    test(
      'init with onReady while init is in flight still sends payload',
      () async {
        final messages = <String>[];
        final ws = WebSocket(
          testServer.uri,
          onMessage: messages.add,
        );

        final first = ws.init();
        final second = ws.init(onReady: 'late-ready');

        await Future.wait([first, second]);
        await ws.connection.firstWhere((s) => s is Connected);
        await waitFor(() => messages.contains('echo late-ready'));
        expect(messages, contains('echo late-ready'));
        await ws.close();
      },
    );

    test('send returns false when not connected', () {
      final ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
      );
      expect(ws.send('not-connected'), isFalse);
    });

    test(
      'connectionAttemptRateLimiter wraps init and reconnect upgrade attempts',
      () async {
        final rateLimiter = RateScheduler(1, const Duration(milliseconds: 25));
        final attemptStarted = <DateTime>[];
        final originalConnector = internal_ws.webSocketConnector;
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };
        addTearDown(() {
          internal_ws.webSocketConnector = originalConnector;
        });

        final ws = WebSocket(
          testServer.uri,
          onMessage: (_) {},
          connectionAttemptRateLimiter: rateLimiter,
          backoff: ConstantBackoff(const Duration(milliseconds: 10)),
          timeout: const Duration(milliseconds: 400),
        );

        await ws.init();
        await ws.connection.firstWhere((s) => s is Connected);
        expect(attemptStarted, hasLength(1));

        unawaited(ws.attemptToReconnect(Exception('trigger reconnect')));
        await ws.connection.firstWhere((s) => s is Reconnecting);
        await ws.connection.firstWhere((s) => s is Reconnected);

        expect(attemptStarted, hasLength(2));
        expect(
          attemptStarted.last.difference(attemptStarted.first),
          greaterThanOrEqualTo(const Duration(milliseconds: 25)),
        );

        await ws.close();
      },
    );

    test(
      'global connectionAttemptRateLimiter is shared across WebSocket instances',
      () async {
        final originalConnector = internal_ws.webSocketConnector;
        final originalGlobalLimiter =
            WebSocket.globalConnectionAttemptRateLimiter;
        final attemptStarted = <DateTime>[];

        WebSocket.globalConnectionAttemptRateLimiter = RateScheduler(
          1,
          const Duration(milliseconds: 25),
        );
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };

        addTearDown(() {
          WebSocket.globalConnectionAttemptRateLimiter = originalGlobalLimiter;
          internal_ws.webSocketConnector = originalConnector;
        });

        final ws1 = WebSocket(testServer.uri, onMessage: (_) {});
        final ws2 = WebSocket(testServer.uri, onMessage: (_) {});

        await Future.wait([ws1.init(), ws2.init()]);
        await Future.wait([
          ws1.connection.firstWhere((s) => s is Connected),
          ws2.connection.firstWhere((s) => s is Connected),
        ]);

        expect(attemptStarted, hasLength(2));
        expect(
          attemptStarted.last.difference(attemptStarted.first),
          greaterThanOrEqualTo(const Duration(milliseconds: 25)),
        );

        await ws1.close();
        await ws2.close();
      },
    );

    test(
      'global connectionAttemptRateLimiter also gates later-created WebSocket instances',
      () async {
        final originalConnector = internal_ws.webSocketConnector;
        final originalGlobalLimiter =
            WebSocket.globalConnectionAttemptRateLimiter;
        final attemptStarted = <DateTime>[];

        WebSocket.globalConnectionAttemptRateLimiter = RateScheduler(
          1,
          const Duration(milliseconds: 25),
        );
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };

        addTearDown(() {
          WebSocket.globalConnectionAttemptRateLimiter = originalGlobalLimiter;
          internal_ws.webSocketConnector = originalConnector;
        });

        final ws1 = WebSocket(testServer.uri, onMessage: (_) {});
        await ws1.init();
        await ws1.connection.firstWhere((s) => s is Connected);

        final ws2 = WebSocket(testServer.uri, onMessage: (_) {});
        final ws3 = WebSocket(testServer.uri, onMessage: (_) {});
        await Future.wait([ws2.init(), ws3.init()]);
        await Future.wait([
          ws2.connection.firstWhere((s) => s is Connected),
          ws3.connection.firstWhere((s) => s is Connected),
        ]);

        expect(attemptStarted, hasLength(3));
        final laterAttempts = attemptStarted.skip(1).toList();
        expect(
          laterAttempts.last.difference(laterAttempts.first),
          greaterThanOrEqualTo(const Duration(milliseconds: 25)),
        );

        await ws1.close();
        await ws2.close();
        await ws3.close();
      },
    );

    test(
      'global connectionAttemptRateLimiter is respected across init and reconnect bursts',
      () async {
        final originalConnector = internal_ws.webSocketConnector;
        final originalGlobalLimiter =
            WebSocket.globalConnectionAttemptRateLimiter;
        final attemptStarted = <DateTime>[];

        WebSocket.globalConnectionAttemptRateLimiter = RateScheduler(
          1,
          const Duration(milliseconds: 25),
        );
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };

        addTearDown(() {
          WebSocket.globalConnectionAttemptRateLimiter = originalGlobalLimiter;
          internal_ws.webSocketConnector = originalConnector;
        });

        final sockets = List.generate(
          3,
          (_) => WebSocket(
            testServer.uri,
            onMessage: (_) {},
            backoff: ConstantBackoff(const Duration(milliseconds: 10)),
            timeout: const Duration(milliseconds: 400),
          ),
        );

        await Future.wait(sockets.map((socket) => socket.init()));
        await Future.wait(
          sockets.map((socket) => socket.connection.firstWhere((s) => s is Connected)),
        );

        expect(attemptStarted, hasLength(3));
        expect(
          attemptStarted.last.difference(attemptStarted.first),
          greaterThanOrEqualTo(const Duration(milliseconds: 40)),
        );

        final baselineAttempts = attemptStarted.length;
        await Future.wait(
          sockets.map(
            (socket) => socket.attemptToReconnect(
              Exception('trigger reconnect burst'),
            ),
          ),
        );
        await Future.wait(
          sockets.map(
            (socket) => socket.connection.firstWhere(
              (s) => s is Reconnected || s is Connected,
            ),
          ),
        );

        final reconnectAttempts = attemptStarted.skip(baselineAttempts).toList();
        expect(reconnectAttempts, hasLength(3));
        expect(
          reconnectAttempts.last.difference(reconnectAttempts.first),
          greaterThanOrEqualTo(const Duration(milliseconds: 40)),
        );

        await Future.wait(sockets.map((socket) => socket.close()));
      },
    );

    test(
      'instance limiter override bypasses global limiter for that WebSocket only',
      () async {
        final originalConnector = internal_ws.webSocketConnector;
        final originalGlobalLimiter =
            WebSocket.globalConnectionAttemptRateLimiter;
        final attemptStarted = <DateTime>[];

        WebSocket.globalConnectionAttemptRateLimiter = RateScheduler(
          1,
          const Duration(milliseconds: 25),
        );
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };

        addTearDown(() {
          WebSocket.globalConnectionAttemptRateLimiter = originalGlobalLimiter;
          internal_ws.webSocketConnector = originalConnector;
        });

        final defaultWs = WebSocket(testServer.uri, onMessage: (_) {});
        final bypassWs = WebSocket(
          testServer.uri,
          onMessage: (_) {},
          useGlobalConnectionAttemptRateLimiter: false,
        );

        await Future.wait([defaultWs.init(), bypassWs.init()]);
        await Future.wait([
          defaultWs.connection.firstWhere((s) => s is Connected),
          bypassWs.connection.firstWhere((s) => s is Connected),
        ]);

        expect(attemptStarted, hasLength(2));
        expect(
          attemptStarted.last.difference(attemptStarted.first),
          lessThan(const Duration(milliseconds: 25)),
        );

        await defaultWs.close();
        await bypassWs.close();
      },
    );

    test(
      'separate instance connectionAttemptRateLimiters do not become global across WebSockets',
      () async {
        final originalConnector = internal_ws.webSocketConnector;
        final originalGlobalLimiter =
            WebSocket.globalConnectionAttemptRateLimiter;
        final attemptStarted = <DateTime>[];

        WebSocket.globalConnectionAttemptRateLimiter = RateScheduler(
          1,
          const Duration(milliseconds: 25),
        );
        internal_ws.webSocketConnector =
            (
              url, {
              protocols,
              headers,
              pingInterval,
            }) {
              attemptStarted.add(DateTime.now());
              return originalConnector(
                url,
                protocols: protocols,
                headers: headers,
                pingInterval: pingInterval,
              );
            };

        addTearDown(() {
          WebSocket.globalConnectionAttemptRateLimiter = originalGlobalLimiter;
          internal_ws.webSocketConnector = originalConnector;
        });

        final ws1 = WebSocket(
          testServer.uri,
          onMessage: (_) {},
          connectionAttemptRateLimiter: RateScheduler(
            1,
            const Duration(milliseconds: 25),
          ),
        );
        final ws2 = WebSocket(
          testServer.uri,
          onMessage: (_) {},
          connectionAttemptRateLimiter: RateScheduler(
            1,
            const Duration(milliseconds: 25),
          ),
        );

        await Future.wait([ws1.init(), ws2.init()]);
        await Future.wait([
          ws1.connection.firstWhere((s) => s is Connected),
          ws2.connection.firstWhere((s) => s is Connected),
        ]);

        expect(attemptStarted, hasLength(2));
        expect(
          attemptStarted.last.difference(attemptStarted.first),
          lessThan(const Duration(milliseconds: 25)),
        );

        await ws1.close();
        await ws2.close();
      },
    );

    test('server initiated close emits close info and reconnects', () async {
      final messages = <String>[];
      final ws = WebSocket(
        testServer.uri,
        onMessage: messages.add,
        backoff: ConstantBackoff(const Duration(milliseconds: 10)),
      );
      await ws.init();
      await ws.connection.firstWhere((s) => s is Connected);

      final disconnectedFuture = ws.connection.firstWhere(
        (s) => s is Disconnected,
      );
      final reconnectedFuture = ws.connection.firstWhere(
        (s) => s is Reconnected,
      );

      ws.send('__server_close__');
      final disconnected = await disconnectedFuture as Disconnected;
      expect(disconnected.code, 4001);
      expect(disconnected.reason, 'server closed connection');

      await reconnectedFuture.timeout(const Duration(seconds: 2));
      ws.send('after reconnect');
      await waitFor(() => messages.contains('echo after reconnect'));
      expect(messages, contains('echo after reconnect'));

      await ws.close();
    });
  });
}
