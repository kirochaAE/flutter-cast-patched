import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'cast_channel/cast_channel.pb.dart';
import 'src/frame_decoder.dart';

class CastSocketMessage {
  const CastSocketMessage(
    this.namespace,
    this.payload, {
    this.sourceId = 'receiver-0',
    this.destinationId = '*',
  });

  final String namespace;
  final Map<String, dynamic> payload;
  final String sourceId;
  final String destinationId;
}

class CastSocket {
  /// Uses an already-connected byte stream, including in host-only tests.
  CastSocket.fromStreams(Stream<List<int>> input, this._output) {
    _subscription = input.listen(
      _handleData,
      onError: _fail,
      onDone: _handleDone,
    );
    unawaited(_output.done.then<void>((_) {}, onError: _fail));
  }

  Stream<CastSocketMessage> get stream => _controller.stream;

  final IOSink _output;
  final _decoder = CastFrameDecoder();
  final _controller = StreamController<CastSocketMessage>.broadcast(sync: true);
  late final StreamSubscription<List<int>> _subscription;
  int _requestId = 0;
  bool _closed = false;
  Future<void>? _closing;

  static Future<CastSocket> connect(
    String host,
    int port, [
    Duration? timeout,
  ]) async {
    final socket = await SecureSocket.connect(
      host,
      port,
      // Chromecast devices use self-signed certificates.
      onBadCertificate: (_) => true,
      timeout: timeout ?? const Duration(seconds: 10),
    );
    return CastSocket.fromStreams(socket, socket);
  }

  void _handleData(List<int> chunk) {
    if (_closed) return;
    try {
      for (final bytes in _decoder.add(chunk)) {
        final message = CastMessage.fromBuffer(bytes);
        if (!message.isInitialized() ||
            message.namespace.isEmpty ||
            message.sourceId.isEmpty ||
            message.destinationId.isEmpty) {
          throw const FormatException('Invalid Cast envelope');
        }
        // Device authentication uses binary protobuf payloads, not JSON.
        if (message.payloadType != CastMessage_PayloadType.STRING) continue;
        final payload = jsonDecode(message.payloadUtf8);
        if (payload is! Map<String, dynamic>) {
          throw const FormatException('Cast payload must be a JSON object');
        }
        _controller.add(CastSocketMessage(
          message.namespace,
          payload,
          sourceId: message.sourceId,
          destinationId: message.destinationId,
        ));
        if (_closed) return;
      }
    } catch (error, stackTrace) {
      _fail(error, stackTrace);
    }
  }

  void _handleDone() {
    if (_closed) return;
    try {
      _decoder.finish();
    } catch (error, stackTrace) {
      _fail(error, stackTrace);
      return;
    }
    unawaited(close());
  }

  void _fail(Object error, [StackTrace? stackTrace]) {
    if (_closed) return;
    _controller.addError(error, stackTrace);
    unawaited(close());
  }

  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    _closed = true;
    await _subscription.cancel();
    // Destroy a real socket so a stalled peer cannot hold teardown open.
    if (_output is Socket) {
      (_output as Socket).destroy();
    } else {
      try {
        await _output.close();
      } catch (_) {
        // The stream error has already been reported by _fail.
      }
    }
    // A paused public subscriber must not hold socket resource teardown open.
    unawaited(_controller.close());
  }

  void sendMessage(
    String namespace,
    String sourceId,
    String destinationId,
    Map<String, dynamic> payload,
  ) {
    if (_closed) throw StateError('Cast socket is closed');
    final outgoing = Map<String, dynamic>.of(payload);
    outgoing.putIfAbsent('requestId', () => _requestId++);
    final message = CastMessage(
      protocolVersion: CastMessage_ProtocolVersion.CASTV2_1_0,
      sourceId: sourceId,
      destinationId: destinationId,
      namespace: namespace,
      payloadType: CastMessage_PayloadType.STRING,
      payloadUtf8: jsonEncode(outgoing),
    );
    final bytes = message.writeToBuffer();
    if (bytes.length > CastFrameDecoder.maxFrameLength) {
      throw ArgumentError('Cast message exceeds maximum frame length');
    }
    final frame = Uint8List(4 + bytes.length);
    ByteData.sublistView(frame).setUint32(0, bytes.length);
    frame.setRange(4, frame.length, bytes);
    _output.add(frame);
  }

  Future<void> flush() => _output.flush();
}
