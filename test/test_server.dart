import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_web_socket/shelf_web_socket.dart';

class TestServer {
  HttpServer? _server;

  Future<void> setupTestServer() async {
    if (_server != null) return;

    final handler = webSocketHandler((webSocket, _) {
      webSocket.stream.listen((message) {
        if (message is String) {
          if (message == '__server_close__') {
            unawaited(webSocket.sink.close(4001, 'server closed connection'));
            return;
          }
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

    _server = await shelf_io.serve(handler, 'localhost', 0);
  }

  Uri get uri {
    final server = _server;
    if (server == null) {
      throw StateError('Test server has not been started.');
    }
    return Uri(
      scheme: 'ws',
      host: server.address.host,
      port: server.port,
    );
  }

  Future<void> close() async {
    if (_server != null) {
      await _server!.close();
      _server = null;
    }
  }
}
