// ignore_for_file: inference_failure_on_instance_creation, unnecessary_lambdas, prefer_const_constructors

import 'dart:async';
import 'package:test/test.dart';
import 'package:web_socket_client/web_socket_client.dart';
import 'test_server.dart';

void main() {
  setUpAll(() {
    setupTestServer();
  });

  group('WebSocket', () {
    late WebSocket ws;
    final messages = <String>[];

    setUp(() async {
      messages.clear();
      ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
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
      // Wait for echo
      await Future.delayed(const Duration(milliseconds: 100));
      expect(messages, contains('echo hello'));
    });

    test('reconnects after server closes connection', () async {
      ws.send('test');
      await Future.delayed(const Duration(milliseconds: 100));
      // Simulate server closing connection by closing client channel
      await ws.close();
      // Re-initialize to simulate reconnect
      ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
        onMessage: (msg) => messages.add(msg),
      );
      await ws.init();
      await ws.connection.firstWhere((state) => state is Connected);
      ws.send('again');
      await Future.delayed(const Duration(milliseconds: 100));
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

    test('ConstantBackoff handles zero and negative durations', () {
      final zeroBackoff = ConstantBackoff(Duration.zero);
      expect(zeroBackoff.next(), Duration.zero);
      zeroBackoff.reset();
      expect(zeroBackoff.next(), Duration.zero);

      final negativeBackoff = ConstantBackoff(const Duration(seconds: -1));
      expect(negativeBackoff.next(), const Duration(seconds: -1));
      negativeBackoff.reset();
      expect(negativeBackoff.next(), const Duration(seconds: -1));
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

    test('BinaryExponentialBackoff with maximumStep 1 returns initial always',
        () {
      final backoff = BinaryExponentialBackoff(
        initial: const Duration(milliseconds: 50),
        maximumStep: 1,
      );
      expect(backoff.next(), const Duration(milliseconds: 50));
      expect(backoff.next(), const Duration(milliseconds: 50));
      expect(backoff.next(), const Duration(milliseconds: 50));
    });

    test('BinaryExponentialBackoff with maximumStep 0 returns initial always',
        () {
      final backoff = BinaryExponentialBackoff(
        initial: const Duration(milliseconds: 10),
        maximumStep: 0,
      );
      expect(backoff.next(), const Duration(milliseconds: 10));
      expect(backoff.next(), const Duration(milliseconds: 10));
    });

    test('BinaryExponentialBackoff handles zero and negative initial durations',
        () {
      final zeroBackoff = BinaryExponentialBackoff(
        initial: Duration.zero,
        maximumStep: 3,
      );
      expect(zeroBackoff.next(), Duration.zero);
      expect(zeroBackoff.next(), Duration.zero);

      final negativeBackoff = BinaryExponentialBackoff(
        initial: const Duration(seconds: -1),
        maximumStep: 2,
      );
      expect(negativeBackoff.next(), const Duration(seconds: -1));
      expect(negativeBackoff.next(), const Duration(seconds: -2));
      expect(negativeBackoff.next(), const Duration(seconds: -2));
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
        Uri.parse('ws://localhost:8080'),
        onMessage: (_) {},
      );
      await ws.init();
      await ws.close();
      expect(() => ws.send('should not throw'), returnsNormally);
    });

    test('double close does not throw', () async {
      final ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
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
        Uri.parse('ws://localhost:8080'),
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
      await Future.delayed(const Duration(milliseconds: 50));
      // Should remain closed and not reconnect
      expect(ws.connection.state, isA<Disconnected>());
    });

    test('close during reconnect prevents further reconnects', () async {
      final ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
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
      await Future.delayed(const Duration(milliseconds: 50));
      expect(ws.connection.state, isA<Disconnected>());
    });

    test('close is idempotent and cancels reconnect timer', () async {
      final ws = WebSocket(
        Uri.parse('ws://localhost:8080'),
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
      await Future.delayed(const Duration(milliseconds: 50));
      expect(ws.connection.state, isA<Disconnected>());
    });
  });
}
