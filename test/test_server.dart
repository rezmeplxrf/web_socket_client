// ignore_for_file: avoid_print, discarded_futures

import 'dart:convert';
import 'dart:io';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_web_socket/shelf_web_socket.dart';

class TestServer {
  HttpServer? _server;

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
      _server = server;
      print('Serving at ws://${server.address.host}:${server.port}');
    });
  }

  Future<void> close() async {
    if (_server != null) {
      await _server!.close();
      _server = null;
      print('Test server closed');
    }
  }
}
