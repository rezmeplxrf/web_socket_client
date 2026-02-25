import 'dart:async';

import 'package:test/test.dart';
import 'package:web_socket_client/src/connection.dart';
import 'package:web_socket_client/web_socket_client.dart';

void main() {
  group('ConnectionController', () {
    test('starts in Connecting state', () {
      final controller = ConnectionController();
      expect(controller.state, isA<Connecting>());
      controller.close();
    });

    test('late subscribers receive current state first', () async {
      final controller = ConnectionController()
        ..add(const Connected())
        ..add(const Reconnecting());

      final state = await controller.first;
      expect(state, isA<Reconnecting>());
      controller.close();
    });

    test(
      'listeners capture rapid transitions right after subscribe',
      () async {
        final controller = ConnectionController();
        final seen = <ConnectionState>[];
        final sub = controller.listen(seen.add);

        controller
          ..add(const Connected())
          ..add(const Reconnecting())
          ..add(const Disconnected(code: 1001, reason: 'y'));

        await Future<void>.delayed(Duration.zero);
        await sub.cancel();
        controller.close();

        expect(
          seen,
          equals(<ConnectionState>[
            const Connecting(),
            const Connected(),
            const Reconnecting(),
            const Disconnected(code: 1001, reason: 'y'),
          ]),
        );
      },
    );

    test(
      'rapid transitions preserve order and suppress consecutive duplicates',
      () async {
        final controller = ConnectionController();
        final seen = <ConnectionState>[];
        final sub = controller.listen(seen.add);
        await Future<void>.delayed(Duration.zero);

        controller
          ..add(const Connecting())
          ..add(const Connecting())
          ..add(const Connected())
          ..add(const Connected())
          ..add(const Reconnecting())
          ..add(const Reconnecting())
          ..add(const Reconnected())
          ..add(const Disconnected(code: 1000, reason: 'x'))
          ..add(const Disconnected(code: 1000, reason: 'x'))
          ..add(const Disconnecting())
          ..add(const Disconnecting())
          ..add(const Disconnected(code: 1001, reason: 'y'));

        await Future<void>.delayed(Duration.zero);
        await sub.cancel();
        controller.close();

        expect(
          seen,
          equals(<ConnectionState>[
            const Connecting(),
            const Connected(),
            const Reconnecting(),
            const Reconnected(),
            const Disconnected(code: 1000, reason: 'x'),
            const Disconnecting(),
            const Disconnected(code: 1001, reason: 'y'),
          ]),
        );
      },
    );

    test(
      'multiple listeners observe identical rapid state sequences',
      () async {
        final controller = ConnectionController();
        final first = <ConnectionState>[];
        final second = <ConnectionState>[];
        final s1 = controller.listen(first.add);
        final s2 = controller.listen(second.add);
        await Future<void>.delayed(Duration.zero);

        controller
          ..add(const Connected())
          ..add(const Reconnecting())
          ..add(const Reconnected())
          ..add(const Disconnecting())
          ..add(const Disconnected(code: 1000, reason: 'done'));

        await Future<void>.delayed(Duration.zero);
        await Future.wait([s1.cancel(), s2.cancel()]);
        controller.close();

        expect(first, equals(second));
        expect(
          first,
          equals(<ConnectionState>[
            const Connecting(),
            const Connected(),
            const Reconnecting(),
            const Reconnected(),
            const Disconnecting(),
            const Disconnected(code: 1000, reason: 'done'),
          ]),
        );
      },
    );

    test('close completes listeners', () async {
      final done = Completer<void>();
      ConnectionController()
        ..listen(null, onDone: done.complete)
        ..close();
      await expectLater(done.future, completes);
    });
  });

  group('ConnectionState equality edge cases', () {
    test('Disconnected equality includes stackTrace and error', () {
      final error = StateError('x');
      final a = Disconnected(
        code: 1006,
        reason: 'abnormal',
        error: error,
        stackTrace: StackTrace.empty,
      );
      final b = Disconnected(
        code: 1006,
        reason: 'abnormal',
        error: error,
        stackTrace: StackTrace.current,
      );

      expect(a == b, isFalse);
    });

    test('Disconnected hashCode matches equality fields', () {
      final error = StateError('x');
      const stackTrace = StackTrace.empty;
      final a = Disconnected(
        code: 1006,
        reason: 'abnormal',
        error: error,
        stackTrace: stackTrace,
      );
      final b = Disconnected(
        code: 1006,
        reason: 'abnormal',
        error: error,
        stackTrace: stackTrace,
      );

      expect(a == b, isTrue);
      expect(a.hashCode, b.hashCode);
    });
  });
}
