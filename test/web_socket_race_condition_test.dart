import 'package:test/test.dart';
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

  group('WebSocket Race Conditions & Error Handling', () {
    late WebSocket ws;

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

    setUp(() {
      ws = WebSocket(
        testServer.uri,
        onMessage: (_) {},
        backoff: const ConstantBackoff(Duration(milliseconds: 100)),
      );
    });

    tearDown(() async {
      await ws.close();
    });

    test('concurrent init calls return the same future', () async {
      final future1 = ws.init();
      final future2 = ws.init();

      expect(future1, same(future2));

      await future1;
      await ws.connection.firstWhere((state) => state is Connected);
      expect(ws.connection.state, isA<Connected>());
    });

    test('subsequent init calls return completed future if connected', () async {
      await ws.init();
      await ws.connection.firstWhere((state) => state is Connected);

      final future1 = ws.init();
      final future2 = ws.init();

      // They might not be the *same* future instance as Future.value() but they should complete immediately.
      // Actually, my implementation returns Future.value() if connected.
      // Let's check if they complete.
      await expectLater(future1, completes);
      await expectLater(future2, completes);
    });

    test('attemptToReconnect preserves stack trace', () async {
      await ws.init();
      await ws.connection.firstWhere((state) => state is Connected);

      final error = Exception('Simulated error');
      final stackTrace = StackTrace.current;

      // We need to capture the Disconnected state
      final disconnectFuture = ws.connection.firstWhere(
        (s) => s is Disconnected,
      );

      // Allow subscription to settle (ConnectionController uses async* which has a delay)
      await Future<void>.delayed(Duration.zero);

      // Trigger reconnection with error and stack trace
      await ws.attemptToReconnect(error, stackTrace);

      final state = await disconnectFuture;
      expect(state, isA<Disconnected>());
      final disconnectedState = state as Disconnected;

      expect(disconnectedState.error, equals(error));
      expect(disconnectedState.stackTrace, equals(stackTrace));
    });

    test(
      'recovers from repeated rapid server-initiated disconnects',
      () async {
        final messages = <String>[];
        final states = <ConnectionState>[];

        final stressWs = WebSocket(
          testServer.uri,
          onMessage: messages.add,
          backoff: const ConstantBackoff(Duration(milliseconds: 10)),
          timeout: const Duration(milliseconds: 400),
        );
        final stateSub = stressWs.connection.listen(states.add);

        try {
          await stressWs.init();
          await stressWs.connection.firstWhere((s) => s is Connected);

          const cycles = 5;
          for (var i = 0; i < cycles; i++) {
            final reconnectFuture = stressWs.connection
                .skip(1)
                .firstWhere(
                  (s) => s is Reconnected,
                );
            final disconnectedFuture = stressWs.connection
                .skip(1)
                .firstWhere(
                  (s) => s is Disconnected,
                );

            stressWs.send('__server_close__');
            final disconnected =
                await disconnectedFuture.timeout(const Duration(seconds: 2))
                    as Disconnected;
            expect(disconnected.code, 4001);

            await reconnectFuture.timeout(const Duration(seconds: 2));
          }

          stressWs.send('stability-check');
          await waitFor(() => messages.contains('echo stability-check'));
          expect(messages, contains('echo stability-check'));

          final reconnectCount = states.whereType<Reconnected>().length;
          final disconnectCount = states.whereType<Disconnected>().length;
          expect(reconnectCount, greaterThanOrEqualTo(cycles));
          expect(disconnectCount, greaterThanOrEqualTo(cycles));
        } finally {
          await stateSub.cancel();
          await stressWs.close();
        }
      },
    );

    test(
      'rapid concurrent reconnect triggers coalesce and still recover',
      () async {
        final states = <ConnectionState>[];
        final messages = <String>[];
        final stressWs = WebSocket(
          testServer.uri,
          onMessage: messages.add,
          backoff: const ConstantBackoff(Duration(milliseconds: 10)),
          timeout: const Duration(milliseconds: 400),
        );
        final stateSub = stressWs.connection.listen(states.add);

        try {
          await stressWs.init();
          await stressWs.connection.firstWhere((s) => s is Connected);

          // Force a server close to enter reconnect flow.
          stressWs.send('__server_close__');
          await stressWs.connection.firstWhere((s) => s is Disconnected);

          // Simulate multiple concurrent error paths trying to reconnect.
          await Future.wait(
            List<Future<void>>.generate(
              10,
              (_) => stressWs.attemptToReconnect(
                Exception('simulated concurrent reconnect'),
              ),
            ),
          );

          await stressWs.connection.skip(1).firstWhere((s) => s is Reconnected);
          stressWs.send('post-burst');
          await waitFor(() => messages.contains('echo post-burst'));
          expect(messages, contains('echo post-burst'));

          final reconnectingCount = states.whereType<Reconnecting>().length;
          final reconnectedCount = states.whereType<Reconnected>().length;
          expect(reconnectingCount, greaterThan(0));
          expect(reconnectedCount, greaterThan(0));
        } finally {
          await stateSub.cancel();
          await stressWs.close();
        }
      },
    );

    test(
      'server disconnect follows strict transition sequence',
      () async {
        final states = <ConnectionState>[];
        final stressWs = WebSocket(
          testServer.uri,
          onMessage: (_) {},
          backoff: const ConstantBackoff(Duration(milliseconds: 10)),
          timeout: const Duration(milliseconds: 400),
        );

        try {
          await stressWs.init();
          await stressWs.connection.firstWhere((s) => s is Connected);
          final sub = stressWs.connection.listen(states.add);

          stressWs.send('__server_close__');
          await stressWs.connection.skip(1).firstWhere((s) => s is Reconnected);
          await Future<void>.delayed(const Duration(milliseconds: 20));
          await sub.cancel();

          final disconnected = states.whereType<Disconnected>().toList();
          expect(disconnected, isNotEmpty);
          expect(disconnected.first.code, 4001);
          expect(disconnected.first.reason, 'server closed connection');

          final tags = states
              .map((state) => state.runtimeType.toString())
              .toList(growable: false);
          final disconnectedIndex = tags.indexOf('Disconnected');
          final reconnectingIndex = tags.indexOf('Reconnecting');
          final reconnectedIndex = tags.indexOf('Reconnected');

          expect(disconnectedIndex, greaterThanOrEqualTo(0));
          expect(reconnectingIndex, greaterThan(disconnectedIndex));
          expect(reconnectedIndex, greaterThan(reconnectingIndex));
        } finally {
          await stressWs.close();
        }
      },
    );
  });
}
