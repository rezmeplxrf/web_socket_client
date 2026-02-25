# WebSocket Client

A reusable Dart `WebSocket` client with reconnect support and connection state
tracking.

This fork targets `dart:io` runtimes (VM/server). Web/wasm is not supported.

## Quick Start

```dart
import 'package:web_socket_client/web_socket_client.dart';

void main() async {
  final socket = WebSocket(
    Uri.parse('ws://localhost:8080'),
    onMessage: (message) => print('message: $message'),
  );

  await socket.init(onReady: 'ping');
  socket.send('hello');

  await socket.close();
}
```

## Connection Lifecycle

The client does not connect in the constructor. Call `init()` to establish a
connection.

```dart
final socket = WebSocket(
  Uri.parse('ws://localhost:8080'),
  onMessage: print,
);

await socket.init();
```

Default connect timeout is `Duration(seconds: 30)`.

## Reconnect Strategy

If connection setup fails or the stream ends unexpectedly, the client attempts
to reconnect using the configured `Backoff`.

Built-in strategies:
- `BinaryExponentialBackoff` (default)
- `ConstantBackoff`
- `NoBackoff` (never reconnect)

```dart
final socket = WebSocket(
  Uri.parse('ws://localhost:8080'),
  onMessage: print,
  backoff: const ConstantBackoff(Duration(seconds: 1)),
);
```

## Connection State

Use `connection` to observe state transitions or inspect the current state.

```dart
socket.connection.listen((state) {
  print('state: $state');
});

final current = socket.connection.state;
```

Possible states:
- `Connecting`
- `Connected`
- `Reconnecting`
- `Reconnected`
- `Disconnecting`
- `Disconnected`

`Disconnected` includes optional `code`, `reason`, `error`, and `stackTrace`.

## Sending Messages

`send` returns `true` when the message is queued and `false` when disconnected
or when the sink throws.

```dart
final sent = socket.send('ping');
```

## Error Hook

Use `onError` to observe internal stream/send/close errors.

```dart
final socket = WebSocket(
  Uri.parse('ws://localhost:8080'),
  onMessage: print,
  onError: (error, stackTrace) {
    print('socket error: $error');
  },
);
```

## Other Message Types

Use `onOtherMessage` to handle non-`String` frames (for example binary payloads).
If `onOtherMessage` is not provided, non-`String` frames are ignored and a log
entry is emitted.

```dart
final socket = WebSocket(
  Uri.parse('ws://localhost:8080'),
  onMessage: print,
  onOtherMessage: (message) {
    print('non-string message: $message (${message.runtimeType})');
  },
);
```

## Closing

Calling `close()`:
- cancels pending reconnect timers
- closes the current sink/subscription
- transitions state to `Disconnected`
- prevents future reconnects for that instance

```dart
await socket.close(1000, 'normal');
```
