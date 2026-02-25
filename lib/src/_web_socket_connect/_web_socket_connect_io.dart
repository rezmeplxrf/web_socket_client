import 'dart:io';

/// Create a WebSocket connection.
Future<WebSocket> connect(
  String url, {
  Iterable<String>? protocols,
  Map<String, dynamic>? headers,
  Duration? pingInterval,
}) async {
  final socket = await WebSocket.connect(
    url,
    headers: headers,
    protocols: protocols,
  );
  socket.pingInterval = pingInterval;
  return socket;
}
