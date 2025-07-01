// ignore_for_file: avoid_print

import 'dart:convert';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_web_socket/shelf_web_socket.dart';

void setupTestServer() {
  final handler = webSocketHandler((webSocket, _) {
    webSocket.stream.listen((message) {
      if (message is String) {
        webSocket.sink.add('echo $message');
      } else if (message is List<int>) {
        // Decode binary as UTF-8 and echo as text
        try {
          final text = utf8.decode(message);
          webSocket.sink.add('echo $text');
        } catch (_) {
          // If not valid UTF-8, echo as bytes
          webSocket.sink.add('echo $message');
        }
      }
    });
  });

  shelf_io.serve(handler, 'localhost', 8080).then((server) {
    print('Serving at ws://${server.address.host}:${server.port}');
  });
}
