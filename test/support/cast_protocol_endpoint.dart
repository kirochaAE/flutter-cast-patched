import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:cast/cast.dart';
import 'package:cast/cast_channel/cast_channel.pb.dart';
import 'package:cast/src/frame_decoder.dart';

class CastProtocolEndpoint {
  CastProtocolEndpoint() {
    _writesSubscription = _writes.stream.listen((chunk) {
      for (final bytes in _decoder.add(chunk)) {
        sent.add(CastMessage.fromBuffer(bytes));
      }
    });
    _output = IOSink(_writes.sink);
    socket = CastSocket.fromStreams(input.stream, _output);
  }

  final input = StreamController<List<int>>(sync: true);
  final _writes = StreamController<List<int>>(sync: true);
  final _decoder = CastFrameDecoder();
  final sent = <CastMessage>[];
  late final CastSocket socket;
  late final IOSink _output;
  late final StreamSubscription<List<int>> _writesSubscription;

  static Uint8List frame(
    String namespace,
    Object? payload, {
    String source = 'receiver-0',
    String destination = 'sender-1',
  }) {
    return frameBytes(
      CastMessage(
        protocolVersion: CastMessage_ProtocolVersion.CASTV2_1_0,
        sourceId: source,
        destinationId: destination,
        namespace: namespace,
        payloadType: CastMessage_PayloadType.STRING,
        payloadUtf8: jsonEncode(payload),
      ).writeToBuffer(),
    );
  }

  static Uint8List frameBytes(List<int> bytes) {
    final frame = Uint8List(4 + bytes.length);
    ByteData.sublistView(frame).setUint32(0, bytes.length);
    frame.setRange(4, frame.length, bytes);
    return frame;
  }

  void receive(
    String namespace,
    Object? payload, {
    String source = 'receiver-0',
    String destination = 'sender-1',
  }) {
    input.add(
      frame(namespace, payload, source: source, destination: destination),
    );
  }

  void status(Object? applications) {
    receive(CastSession.kNamespaceReceiver, {
      'type': 'RECEIVER_STATUS',
      'status': {'applications': applications},
    });
  }

  Map<String, String> application(
    String transport, {
    String appId = 'CC1AD845',
    String sessionId = 'app-session-1',
  }) =>
      {'transportId': transport, 'appId': appId, 'sessionId': sessionId};

  Future<void> flush() => socket.flush();

  Future<void> dispose() async {
    await socket.close();
    await _output.close();
    await _writes.close();
    await input.close();
    await _writesSubscription.cancel();
  }
}
