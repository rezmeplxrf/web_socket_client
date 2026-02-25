// ignore_for_file: prefer_const_constructors

import 'package:test/test.dart';
import 'package:web_socket_client/web_socket_client.dart';

void main() {
  group('BinaryExponentialBackoff', () {
    test('implements Backoff', () {
      expect(
        BinaryExponentialBackoff(
          initial: Duration(milliseconds: 100),
          maximumStep: 3,
        ),
        isA<Backoff>(),
      );
    });

    test('returns initial duration on first call', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 100),
        maximumStep: 7,
      );

      expect(backoff.next(), equals(Duration(milliseconds: 100)));
    });

    test('doubles duration on each successive call', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 100),
        maximumStep: 7,
      );

      expect(backoff.next(), equals(Duration(milliseconds: 100)));
      expect(backoff.next(), equals(Duration(milliseconds: 200)));
      expect(backoff.next(), equals(Duration(milliseconds: 400)));
      expect(backoff.next(), equals(Duration(milliseconds: 800)));
      expect(backoff.next(), equals(Duration(milliseconds: 1600)));
      expect(backoff.next(), equals(Duration(milliseconds: 3200)));
      expect(backoff.next(), equals(Duration(milliseconds: 6400)));
    });

    test('caps at maximum step and does not grow further', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 100),
        maximumStep: 7,
      );

      // Exhaust all steps
      for (var i = 0; i < 7; i++) {
        backoff.next();
      }

      // All subsequent calls should return the capped value
      expect(backoff.next(), equals(Duration(milliseconds: 6400)));
      expect(backoff.next(), equals(Duration(milliseconds: 6400)));
      expect(backoff.next(), equals(Duration(milliseconds: 6400)));
    });

    test('reset restores initial state', () {
      final backoff =
          BinaryExponentialBackoff(
              initial: Duration(milliseconds: 100),
              maximumStep: 7,
            )
            ..next() // 100ms
            ..next() // 200ms
            ..next() // 400ms
            ..reset();

      final first = backoff.next();
      final second = backoff.next();
      expect(first, equals(Duration(milliseconds: 100)));
      expect(second, equals(Duration(milliseconds: 200)));
    });

    test('reset after reaching cap restores full doubling sequence', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 50),
        maximumStep: 3,
      );

      // Reach the cap
      expect(backoff.next(), equals(Duration(milliseconds: 50)));
      expect(backoff.next(), equals(Duration(milliseconds: 100)));
      expect(backoff.next(), equals(Duration(milliseconds: 200)));
      expect(backoff.next(), equals(Duration(milliseconds: 200))); // capped

      backoff.reset();

      // Should restart from initial
      expect(backoff.next(), equals(Duration(milliseconds: 50)));
      expect(backoff.next(), equals(Duration(milliseconds: 100)));
      expect(backoff.next(), equals(Duration(milliseconds: 200)));
      expect(
        backoff.next(),
        equals(Duration(milliseconds: 200)),
      ); // capped again
    });

    test('maximumStep of 1 never doubles', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 500),
        maximumStep: 1,
      );

      expect(backoff.next(), equals(Duration(milliseconds: 500)));
      expect(backoff.next(), equals(Duration(milliseconds: 500)));
      expect(backoff.next(), equals(Duration(milliseconds: 500)));
    });

    test('maximumStep of 2 doubles once then caps', () {
      final backoff = BinaryExponentialBackoff(
        initial: Duration(milliseconds: 100),
        maximumStep: 2,
      );

      expect(backoff.next(), equals(Duration(milliseconds: 100)));
      expect(backoff.next(), equals(Duration(milliseconds: 200)));
      expect(backoff.next(), equals(Duration(milliseconds: 200))); // capped
    });

    test('max value equals initial * 2^(maximumStep - 1)', () {
      const initial = Duration(milliseconds: 100);
      const maximumStep = 5;
      final backoff = BinaryExponentialBackoff(
        initial: initial,
        maximumStep: maximumStep,
      );

      // Consume all steps
      var last = Duration.zero;
      for (var i = 0; i < maximumStep; i++) {
        last = backoff.next();
      }

      // Max = 100 * 2^(5-1) = 100 * 16 = 1600ms
      expect(last, equals(Duration(milliseconds: 1600)));
      // Subsequent calls remain capped
      expect(backoff.next(), equals(Duration(milliseconds: 1600)));
    });

    test('asserts on non-positive maximumStep', () {
      expect(
        () => BinaryExponentialBackoff(
          initial: Duration(milliseconds: 100),
          maximumStep: 0,
        ),
        throwsA(isA<AssertionError>()),
      );
    });

    test('asserts on negative initial duration', () {
      expect(
        () => BinaryExponentialBackoff(
          initial: Duration(seconds: -1),
          maximumStep: 2,
        ),
        throwsA(isA<AssertionError>()),
      );
    });
  });
}
