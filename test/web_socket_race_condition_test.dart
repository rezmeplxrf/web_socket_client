import 'package:test/test.dart';
import 'package:web_socket_client/web_socket_client.dart';
import 'test_server.dart';

void main() {
  final testServer = TestServer();
  setUpAll(testServer.setupTestServer);
  tearDownAll(testServer.close);

  group('WebSocket Race Conditions & Error Handling', () {
    late WebSocket ws;

    setUp(() {
      ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
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
  });
}
