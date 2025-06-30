import 'package:web_socket_client/web_socket_client.dart';

void main() async {
  final ws = WebSocket(Uri.parse('wss://realtime.insightsentry.com/newsfeed'),
      onMessage: (message) {
    print('Received message: $message');
  });
  await ws.init();
  ws.send('ping');
}
