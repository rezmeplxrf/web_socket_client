// Using print just for demonstrative purposes.
// ignore_for_file: avoid_print
import 'package:web_socket_client/web_socket_client.dart';

void main() async {
  // Create a WebSocket client.
  final uri = Uri.parse('ws://localhost:8080');
  const backoff = ConstantBackoff(Duration(seconds: 1));
  WebSocket? socket;
  socket = WebSocket(
    uri,
    backoff: backoff,
    onMessage: (message) {
      print('message: "$message"');
      socket?.send('ping');
    },
  );

  // Listen for changes in the connection state.
  socket.connection.listen((state) => print('state: "$state"'));
  await socket.init(onReady: 'ping');

  await Future<void>.delayed(const Duration(seconds: 3));

  // Close the connection.
  await socket.close();
}
